#include "AdvancedThreadPool.hpp"

#include "Logger.hpp"
#include "SignalUtils.hpp"
#include "TimeUtils.hpp"

#include <cerrno>
#include <cstring>

AdvancedThreadPool::AdvancedThreadPool(std::size_t worker_count, std::size_t queue_capacity)
    : accepting_(true),
      stop_requested_(false),
      shutdown_started_(false),
      next_sequence_(0),
      paused_(false) {
    if (worker_count == 0) {
        throw std::runtime_error("worker_count must be greater than 0");
    }

    if (queue_capacity == 0) {
        throw std::runtime_error("queue_capacity must be greater than 0");
    }

    if (sem_init(&empty_slots_, 0, static_cast<unsigned int>(queue_capacity)) == -1) {
        throw std::runtime_error(std::string("sem_init failed: ") + std::strerror(errno));
    }

    sem_initialized_ = true;

    for (std::size_t i = 0; i < worker_count; ++i) {
        workers_.emplace_back(&AdvancedThreadPool::workerLoop, this, i);
    }

    logLine(
        "[pool] start, workers=" + std::to_string(worker_count) +
        ", queue_capacity=" + std::to_string(queue_capacity)
    );
}

AdvancedThreadPool::~AdvancedThreadPool() {
    shutdown(true);

    if (sem_initialized_) {
        sem_destroy(&empty_slots_);
        sem_initialized_ = false;
    }
}

void AdvancedThreadPool::pause() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        paused_ = true;
    }

    logLine("[pool] paused");
}

void AdvancedThreadPool::resume() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        paused_ = false;
    }

    task_cv_.notify_all();
    logLine("[pool] resumed");
}

void AdvancedThreadPool::shutdown(bool drain_remaining_tasks) {
    bool expected = false;

    if (!shutdown_started_.compare_exchange_strong(expected, true)) {
        return;
    }

    accepting_.store(false);

    std::size_t dropped_tasks = 0;

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);

        if (!drain_remaining_tasks) {
            dropped_tasks = tasks_.size();

            while (!tasks_.empty()) {
                tasks_.pop();
                sem_post(&empty_slots_);
            }
        }

        paused_ = false;
        stop_requested_.store(true);
    }

    if (dropped_tasks > 0) {
        std::unique_lock<std::shared_mutex> lock(stats_mutex_);
        rejected_tasks_ += dropped_tasks;
    }

    task_cv_.notify_all();

    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    logLine("[pool] shutdown finished");
}

void AdvancedThreadPool::recordSignal(int sig) {
    {
        std::unique_lock<std::shared_mutex> lock(stats_mutex_);
        ++received_signals_;
    }

    logLine("[pool] received " + signalName(sig));
}

AdvancedThreadPool::Snapshot AdvancedThreadPool::snapshot() const {
    Snapshot snapshot;

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        snapshot.queued_tasks = tasks_.size();
        snapshot.paused = paused_;
    }

    {
        std::shared_lock<std::shared_mutex> lock(stats_mutex_);
        snapshot.submitted_tasks = submitted_tasks_;
        snapshot.completed_tasks = completed_tasks_;
        snapshot.failed_tasks = failed_tasks_;
        snapshot.rejected_tasks = rejected_tasks_;
        snapshot.received_signals = received_signals_;
        snapshot.active_workers = active_workers_;
    }

    snapshot.accepting = accepting_.load();
    snapshot.stopping = stop_requested_.load();

    return snapshot;
}

bool AdvancedThreadPool::waitEmptySlot() {
    while (accepting_.load() && !stop_requested_.load()) {
        timespec timeout = makeAbsTimeoutMs(100);

        int ret = sem_timedwait(&empty_slots_, &timeout);

        if (ret == 0) {
            return true;
        }

        if (errno == EINTR) {
            continue;
        }

        if (errno == ETIMEDOUT) {
            continue;
        }

        logLine(std::string("[pool] sem_timedwait failed: ") + std::strerror(errno));
        return false;
    }

    return false;
}

void AdvancedThreadPool::workerLoop(std::size_t worker_id) {
    while (true) {
        TaskItem task;
        bool has_task = false;

        {
            std::unique_lock<std::mutex> lock(queue_mutex_);

            task_cv_.wait(lock, [this]() {
                return stop_requested_.load()
                    || (!paused_ && !tasks_.empty());
            });

            if (paused_ && !stop_requested_.load()) {
                continue;
            }

            if (tasks_.empty()) {
                if (stop_requested_.load()) {
                    break;
                }

                continue;
            }

            task = tasks_.top();
            tasks_.pop();
            has_task = true;
        }

        if (has_task) {
            sem_post(&empty_slots_);
        }

        {
            std::unique_lock<std::shared_mutex> lock(stats_mutex_);
            ++active_workers_;
        }

        logLine(
            "[worker " + std::to_string(worker_id) +
            "] start task=" + task.name +
            ", priority=" + std::to_string(task.priority)
        );

        try {
            task.job();

            {
                std::unique_lock<std::shared_mutex> lock(stats_mutex_);
                ++completed_tasks_;
            }

            logLine(
                "[worker " + std::to_string(worker_id) +
                "] finish task=" + task.name
            );
        } catch (const std::exception& e) {
            {
                std::unique_lock<std::shared_mutex> lock(stats_mutex_);
                ++failed_tasks_;
            }

            logLine(
                "[worker " + std::to_string(worker_id) +
                "] task failed=" + task.name +
                ", error=" + e.what()
            );
        } catch (...) {
            {
                std::unique_lock<std::shared_mutex> lock(stats_mutex_);
                ++failed_tasks_;
            }

            logLine(
                "[worker " + std::to_string(worker_id) +
                "] task failed=" + task.name +
                ", unknown error"
            );
        }

        {
            std::unique_lock<std::shared_mutex> lock(stats_mutex_);
            --active_workers_;
        }
    }

    logLine("[worker " + std::to_string(worker_id) + "] exit");
}

void AdvancedThreadPool::increaseRejected() {
    std::unique_lock<std::shared_mutex> lock(stats_mutex_);
    ++rejected_tasks_;
}
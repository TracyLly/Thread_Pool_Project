#ifndef ADVANCED_THREAD_POOL_HPP
#define ADVANCED_THREAD_POOL_HPP

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <semaphore.h>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

class AdvancedThreadPool {
public:
    struct Snapshot {
        std::size_t queued_tasks = 0;
        std::uint64_t submitted_tasks = 0;
        std::uint64_t completed_tasks = 0;
        std::uint64_t failed_tasks = 0;
        std::uint64_t rejected_tasks = 0;
        std::uint64_t received_signals = 0;
        std::size_t active_workers = 0;
        bool accepting = false;
        bool stopping = false;
        bool paused = false;
    };

private:
    struct TaskItem {
        int priority = 0;
        std::uint64_t sequence = 0;
        std::string name;
        std::function<void()> job;
    };

    struct TaskCompare {
        bool operator()(const TaskItem& left, const TaskItem& right) const {
            if (left.priority == right.priority) {
                return left.sequence > right.sequence;
            }

            return left.priority < right.priority;
        }
    };

private:
    std::priority_queue<TaskItem, std::vector<TaskItem>, TaskCompare> tasks_;

    mutable std::mutex queue_mutex_;
    std::condition_variable task_cv_;

    sem_t empty_slots_;
    bool sem_initialized_ = false;

    std::vector<std::thread> workers_;

    std::atomic<bool> accepting_;
    std::atomic<bool> stop_requested_;
    std::atomic<bool> shutdown_started_;
    std::atomic<std::uint64_t> next_sequence_;

    bool paused_;

    mutable std::shared_mutex stats_mutex_;
    std::uint64_t submitted_tasks_ = 0;
    std::uint64_t completed_tasks_ = 0;
    std::uint64_t failed_tasks_ = 0;
    std::uint64_t rejected_tasks_ = 0;
    std::uint64_t received_signals_ = 0;
    std::size_t active_workers_ = 0;

public:
    AdvancedThreadPool(std::size_t worker_count, std::size_t queue_capacity);

    ~AdvancedThreadPool();

    AdvancedThreadPool(const AdvancedThreadPool&) = delete;
    AdvancedThreadPool& operator=(const AdvancedThreadPool&) = delete;

    template <typename F, typename... Args>
    auto submit(std::string task_name, int priority, F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>>;

    void pause();

    void resume();

    void shutdown(bool drain_remaining_tasks);

    void recordSignal(int sig);

    Snapshot snapshot() const;

private:
    bool waitEmptySlot();

    void workerLoop(std::size_t worker_id);

    void increaseRejected();
};

template <typename F, typename... Args>
auto AdvancedThreadPool::submit(std::string task_name, int priority, F&& f, Args&&... args)
    -> std::future<std::invoke_result_t<F, Args...>> {
    using ReturnType = std::invoke_result_t<F, Args...>;

    if (!accepting_.load() || stop_requested_.load()) {
        increaseRejected();
        throw std::runtime_error("thread pool is not accepting new tasks");
    }

    auto promise = std::make_shared<std::promise<ReturnType>>();
    std::future<ReturnType> result = promise->get_future();

    auto bound_task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);

    TaskItem item;
    item.priority = priority;
    item.sequence = next_sequence_.fetch_add(1);
    item.name = std::move(task_name);
    item.job = [promise, bound_task = std::move(bound_task)]() mutable {
        try {
            if constexpr (std::is_void_v<ReturnType>) {
                bound_task();
                promise->set_value();
            } else {
                promise->set_value(bound_task());
            }
        } catch (...) {
            promise->set_exception(std::current_exception());
            throw;
        }
    };

    if (!waitEmptySlot()) {
        increaseRejected();
        throw std::runtime_error("failed to acquire queue slot");
    }

    bool queued = false;

    try {
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);

            if (!accepting_.load() || stop_requested_.load()) {
                increaseRejected();
                throw std::runtime_error("thread pool stopped before task enqueue");
            }

            tasks_.push(std::move(item));
            queued = true;
        }

        {
            std::unique_lock<std::shared_mutex> lock(stats_mutex_);
            ++submitted_tasks_;
        }

        task_cv_.notify_one();
    } catch (...) {
        if (!queued) {
            sem_post(&empty_slots_);
        }

        throw;
    }

    return result;
}

#endif
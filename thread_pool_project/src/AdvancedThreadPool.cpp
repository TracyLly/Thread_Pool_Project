#include "AdvancedThreadPool.hpp"

#include "Logger.hpp"
#include "SignalUtils.hpp"
#include "TimeUtils.hpp"

#include <cerrno>
#include <cstring>

AdvancedThreadPool::AdvancedThreadPool(std::size_t worker_count, std::size_t queue_capacity)
    //创建线程池，worker_count表示线程池数量，queue_capacity表示任务队列最大容量
    : accepting_(true), //表示线程池刚创建，允许接收新任务
      stop_requested_(false), //表示还没有停止请求线程池
      shutdown_started_(false), //表示关闭流程还没有开始
      next_sequence_(0), //任务序号从0开始
      paused_(false) { //线程池刚创建时不是暂停状态
    //构造函数的初始化列表
    if (worker_count == 0) {
        throw std::runtime_error("worker_count must be greater than 0");
    }

    if (queue_capacity == 0) {
        throw std::runtime_error("queue_capacity must be greater than 0");
    }

    if (sem_init(&empty_slots_, 0, static_cast<unsigned int>(queue_capacity)) == -1) {
        throw std::runtime_error(std::string("sem_init failed: ") + std::strerror(errno));
    } //初始化信号量empty_slots_，0表示这个信号量用于同一个进程内的多个线程之间，第三个参数表示信号量初始值

    sem_initialized_ = true;

    for (std::size_t i = 0; i < worker_count; ++i) {
        workers_.emplace_back(&AdvancedThreadPool::workerLoop, this, i);
    } //创建worker线程，并添加到workers_数组中

    logLine(
        "[pool] start, workers=" + std::to_string(worker_count) +
        ", queue_capacity=" + std::to_string(queue_capacity)
    );
}

AdvancedThreadPool::~AdvancedThreadPool() { //线程池析构函数，当线程池对象生命周期结束时，会自动调用
    shutdown(true); //析构时先关闭线程池，可以防止线程池对象销毁时，worker线程还在运行

    if (sem_initialized_) {
        sem_destroy(&empty_slots_); //信号量初始化成功，则销毁信号量
        sem_initialized_ = false;
    }
}

void AdvancedThreadPool::pause() { //暂停线程池，之后worker线程不会继续从任务队列里取新任务
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        //加锁，保护paused_。由于worker线程也会读取paused_，所以对paused_的修改要放在queue_mutex_保护下
        paused_ = true; //设置线程池为暂停状态
    }

    logLine("[pool] paused");
}

void AdvancedThreadPool::resume() { //恢复线程池，恢复后，worker可以继续取任务执行
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        //加锁后，把paused_设置为false，表示线程池恢复
        paused_ = false;
    }

    task_cv_.notify_all(); //唤醒所有等待中的worker
    //因为暂停期间，worker可能睡在条件变量上
    logLine("[pool] resumed");
}

void AdvancedThreadPool::shutdown(bool drain_remaining_tasks) {
    //关闭线程池
    bool expected = false;

    if (!shutdown_started_.compare_exchange_strong(expected, true)) {
        return;
    }
    /*如果shutdown_started_当前值等于expected，也就是false，就把shutdown_started_改成true，并返回true
    如果shutdown_started_已经是true，说明别的线程已经开始关闭了，当前线程就直接return*/
    //可以保证shutdown的核心关闭逻辑只执行一次，否则多个线程同时调用shutdown()，可能会重复join同一个线程，导致错误

    accepting_.store(false); //设置线程池不再接受新任务

    std::size_t dropped_tasks = 0;
    //定义被丢弃的任务数量，如果drain_remaining_tasks == false，会清空任务队列，被清掉的任务数量就记录在这里

    {
        std::lock_guard<std::mutex> lock(queue_mutex_); //加锁，后面要访问tasks_和paused_

        if (!drain_remaining_tasks) { //如果drain_remaining_tasks == false，直接清空任务队列
            dropped_tasks = tasks_.size(); //记录当前队列里还有多少任务没执行

            while (!tasks_.empty()) {
                tasks_.pop(); //丢弃一个任务
                sem_post(&empty_slots_); //释放一个队列空槽
            }
        }

        paused_ = false;
        stop_requested_.store(true);
    }

    if (dropped_tasks > 0) {
        std::unique_lock<std::shared_mutex> lock(stats_mutex_);
        rejected_tasks_ += dropped_tasks; //把丢弃的任务数量统计到rejected_tasks_中
    }

    task_cv_.notify_all(); //唤醒所有worker线程

    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    logLine("[pool] shutdown finished");
}

void AdvancedThreadPool::recordSignal(int sig) { //记录线程池收到的Linux信号
    {
        std::unique_lock<std::shared_mutex> lock(stats_mutex_);
        ++received_signals_; //收到信号数量加1
    }

    logLine("[pool] received " + signalName(sig));
}

AdvancedThreadPool::Snapshot AdvancedThreadPool::snapshot() const {
    //获取线程池状态快照，const表示不修改当前对象的任何非静态成员变量
    Snapshot snapshot;

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        snapshot.queued_tasks = tasks_.size();
        snapshot.paused = paused_;
    }

    {
        std::shared_lock<std::shared_mutex> lock(stats_mutex_);
        //shared_lock是读写锁的共享模式(读锁)，允许多个线程同时持有，不允许并发写，适用于只读操作
        snapshot.submitted_tasks = submitted_tasks_; //记录已经提交的任务数量
        snapshot.completed_tasks = completed_tasks_; //记录已经成功完成的任务数量
        snapshot.failed_tasks = failed_tasks_; //记录执行失败的任务数量
        snapshot.rejected_tasks = rejected_tasks_; //记录被拒绝的任务数量
        snapshot.received_signals = received_signals_; //记录收到的信号数量
        snapshot.active_workers = active_workers_; //记录当前正在执行任务的worker数量
    }

    snapshot.accepting = accepting_.load(); //读取线程池是否还接受任务
    snapshot.stopping = stop_requested_.load(); //读取线程池是否正在停止

    return snapshot;
}

bool AdvancedThreadPool::waitEmptySlot() { //等待任务队列空槽
    while (accepting_.load() && !stop_requested_.load()) {
        //只要线程池还接收任务，并且没有请求停止，就继续尝试等待空槽
        timespec timeout = makeAbsTimeoutMs(100); //生成一个100ms后的绝对时间点

        int ret = sem_timedwait(&empty_slots_, &timeout); //等待信号量
        //如果empty_slots_ > 0：信号量减1，返回0，表示成功获得一个队列空槽。
        //如果empty_slots_ == 0：队列满了，最多等待到timeout。

        if (ret == 0) {
            return true;
        }
        //errno是一个全局，用于存储最近一次系统调用或库函数执行失败时的错误代码

        if (errno == EINTR) { //系统调用被信号中断，continue回到while
            continue;
        }

        if (errno == ETIMEDOUT) { //操作超时，continue回到while重新检查accepting_ 和 stop_requested_
            continue;
        }

        logLine(std::string("[pool] sem_timedwait failed: ") + std::strerror(errno));
        return false;
    }

    return false;
}

void AdvancedThreadPool::workerLoop(std::size_t worker_id) { //worker线程的主循环函数
    while (true) { //worker不断取任务、执行任务
        TaskItem task; //定义一个任务对象，用来保存从队列中取出的任务
        bool has_task = false; //标记是否成功取到了任务

        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            //加队列锁，用unique_lock，后面配合条件变量wait()

            task_cv_.wait(lock, [this]() {
            //task.cv：std::condition_variable 对象，用于线程间的通知机制
            //wait：让工作线程在没有任务可处理时进入休眠状态（释放 CPU），并在有任务到来或需要停止时被唤醒
            //lock：std::unique_lock<std::mutex>，它在调用 wait 之前已经锁住了 queue_mutex_
            //Lambda 表达式 [this]() { ... }: 这是一个谓词（Predicate），定义了“什么情况下线程应该醒来并继续执行”。

            /*检查条件：首先执行Lambda 表达式。如果返回 true，wait 立即返回，线程继续执行后续代码（取任务）。
              释放锁并休眠：如果返回 false（即：没任务、或者被暂停、或者没请求停止），wait会自动释放lock，然后将当前线程放入等待队列并阻塞休眠。
              被唤醒：当其他线程调用 task_cv_.notify_one() 或 notify_all() 时，操作系统唤醒该线程。
              重新加锁并重试：线程被唤醒后，wait 会重新获取 lock，然后再次执行 Lambda 表达式检查条件。
              如果条件仍为 false（例如发生了虚假唤醒，或者多个线程被唤醒但只有一个能抢到任务），线程会再次释放锁并休眠。
              只有当条件为 true 时，wait 才会正式返回，此时线程持有锁。
              */

            //如果条件不满足，释放queue_mutex_，worker线程阻塞睡眠
            //被唤醒后，重新加锁，再次检查条件
                return stop_requested_.load()
                    || (!paused_ && !tasks_.empty());
                //只有线程池请求停止或者线程池没有暂停并且任务队列非空，worker才继续
            });

            if (paused_ && !stop_requested_.load()) {
                continue; //如果线程池暂停，并且还没有请求停止，就继续下一轮循环
            }

            if (tasks_.empty()) {
                if (stop_requested_.load()) {
                    break;
                } //队列为空并且线程池请求停止，那么worker退出循环

                continue; //队列为空但还没请求停止，就继续下一轮等待
            }

            task = tasks_.top(); //从优先级队列中取出优先级最高的任务
            tasks_.pop();
            has_task = true;
        }

        if (has_task) {
            sem_post(&empty_slots_); //成功取走任务，释放空槽
        }

        {
            std::unique_lock<std::shared_mutex> lock(stats_mutex_);
            ++active_workers_; //当前正在工作的worker数量加1
        }

        logLine(
            "[worker " + std::to_string(worker_id) +
            "] start task=" + task.name +
            ", priority=" + std::to_string(task.priority)
        );

        try {
            task.job(); //执行任务

            {
                std::unique_lock<std::shared_mutex> lock(stats_mutex_);
                ++completed_tasks_; //任务执行成功，成功任务数量加1
            }

            logLine(
                "[worker " + std::to_string(worker_id) +
                "] finish task=" + task.name
            );
        } catch (const std::exception& e) { //捕获标准异常，比如throw std::runtime_error("xxx")
            {
                std::unique_lock<std::shared_mutex> lock(stats_mutex_);
                ++failed_tasks_; //失败任务数量加1
            }

            logLine(
                "[worker " + std::to_string(worker_id) +
                "] task failed=" + task.name +
                ", error=" + e.what()
            );
        } catch (...) { //捕获非标准异常，比如throw 1；throw "error"
            {
                std::unique_lock<std::shared_mutex> lock(stats_mutex_);
                ++failed_tasks_; //失败任务数量加1
            }

            logLine(
                "[worker " + std::to_string(worker_id) +
                "] task failed=" + task.name +
                ", unknown error"
            );
        }

        {
            std::unique_lock<std::shared_mutex> lock(stats_mutex_);
            --active_workers_; //当前worker任务执行完毕，不再处于活跃状态
        }
    }

    logLine("[worker " + std::to_string(worker_id) + "] exit");
}

void AdvancedThreadPool::increaseRejected() { //增加拒绝任务数量，在submit()失败时调用
    std::unique_lock<std::shared_mutex> lock(stats_mutex_);
    ++rejected_tasks_;
}
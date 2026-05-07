#ifndef ADVANCED_THREAD_POOL_HPP
#define ADVANCED_THREAD_POOL_HPP

#include <atomic> 
#include <condition_variable> 
#include <cstddef> //引入标准类型，如std::size_t
#include <cstdint> //引入固定宽度整数类型，如std::uint64_t
#include <exception> //引入异常相关，如std::current_exception
#include <functional> //引入函数对象相关，如std::function,std::bind
#include <future> //引入异步结果相关，如std::future,std::promise
#include <memory> //引入智能指针，如std::shared_ptr,std::unique_ptr
#include <mutex> //引入互斥锁
#include <queue> 
#include <semaphore.h> //引入POSIX信号量
#include <shared_mutex> //引入共享锁
#include <stdexcept> //引入标准异常类，如std::runtime_error
#include <string> 
#include <thread>
#include <type_traits> //引入类型萃取工具，如std::invoke_result_t,std::is_void_v
#include <utility> //引入移动语义和完美转发相关工具，std::move,std::forward
#include <vector>

class AdvancedThreadPool {
//线程池类封装了1.任务队列；2.worker线程；3.任务提交接口；4.线程池暂停/恢复；5.线程池关闭；6.统计信息；7.队列容量控制
public:
    struct Snapshot { //线程池状态快照结构体，用于一次性获取线程池当前运行状态
        std::size_t queued_tasks = 0; //当前队列中等待执行的任务数量
        std::uint64_t submitted_tasks = 0; //已经成功提交到线程池的任务总数
        std::uint64_t completed_tasks = 0; //成功执行完成的任务数量
        std::uint64_t failed_tasks = 0; //执行失败的任务数量
        std::uint64_t rejected_tasks = 0; //被拒绝的任务数量
        std::uint64_t received_signals = 0; //线程池收到的Linux信号数量
        std::size_t active_workers = 0; //当前正在执行任务的worker数量
        bool accepting = false; //表示当前线程池是否还接收新任务
        bool stopping = false; //表示线程池是否正在停止
        bool paused = false; //表示线程池是否处于暂停状态
    };

private:
    struct TaskItem { //表示线程池队列中的一个任务
        //线程池内部不是直接保存用户传入的函数，而是把函数包装成TaskItem
        int priority = 0; //任务优先级
        std::uint64_t sequence = 0; //任务序号，相同优先级下，序号小的先执行
        std::string name; //任务名称
        std::function<void()> job; //真正要执行的任务
        //可以存储、复制和调用任何可调用对象：普通函数指针、Lambda、函数对象、成员函数指针等
    };

    struct TaskCompare { //定义任务比较规则
        bool operator()(const TaskItem& left, const TaskItem& right) const {
            if (left.priority == right.priority) {
                return left.sequence > right.sequence;
            }

            return left.priority < right.priority;
        }
    };

private:
    std::priority_queue<TaskItem, std::vector<TaskItem>, TaskCompare> tasks_;
    //线程池的任务队列(优先级队列)
    mutable std::mutex queue_mutex_; //允许在const成员函数中修改被标记的成员变量
    //const成员函数不能修改类的任何成员变量
    std::condition_variable task_cv_; //条件变量
    //worker线程没任务时等待；submit提交任务后唤醒worker；shutdown时唤醒所有worker退出

    sem_t empty_slots_; //POSIX信号量，表示任务队列还剩多少空位置
    bool sem_initialized_ = false; //表示信号量是否已经初始化成功
    //析构函数中会调用sem_destroy(&empty_slots_);但只有信号量初始化成功后才能销毁，避免销毁未初始化的信号量

    std::vector<std::thread> workers_; //保存线程池内部的worker线程

    std::atomic<bool> accepting_; //表示当前线程池是否还接收新任务
    std::atomic<bool> stop_requested_; //表示是否请求worker停止
    std::atomic<bool> shutdown_started_; //表示是否开始执行关闭流程
    std::atomic<std::uint64_t> next_sequence_; //任务序号生成器，这个序号用于同优先级任务的先后顺序

    bool paused_; //表示线程池是否暂停

    mutable std::shared_mutex stats_mutex_; //保护统计信息的读写锁
    std::uint64_t submitted_tasks_ = 0; //已经成功提交的任务数量
    std::uint64_t completed_tasks_ = 0; //成功执行完成的任务数量
    std::uint64_t failed_tasks_ = 0; //执行失败的任务数量
    std::uint64_t rejected_tasks_ = 0; //被拒绝的任务数量
    std::uint64_t received_signals_ = 0; //线程池收到的Linux信号数量
    std::size_t active_workers_ = 0; // //当前正在执行任务的worker数量

public:
    AdvancedThreadPool(std::size_t worker_count, std::size_t queue_capacity);
    //构造函数，worker_count表示worker线程数量，queue_capacity表示任务队列容量
    ~AdvancedThreadPool();  //析构函数,关闭线程池，join所有worker线程，销毁信号量

    AdvancedThreadPool(const AdvancedThreadPool&) = delete; //禁止拷贝构造
    AdvancedThreadPool& operator=(const AdvancedThreadPool&) = delete; //禁止拷贝赋值

    template <typename F, typename... Args>
    auto submit(std::string task_name, int priority, F&& f, Args&&... args)
        -> std::future<std::invoke_result_t<F, Args...>>;
    //提交任务函数，task_name任务名称，priority任务优先级，f要执行的函数，args函数参数
    //尾置返回一个future，其中保存任务函数的返回结果
    //std::invoke_result_t是一个类型萃取工具，它在编译期计算：如果我调用函数 F 并传入参数 Args...，它会返回什么类型
    //std::future代表一个“未来”才会产生的值，比如一个线程执行完任务后，会返回一个future，这个future会保存任务函数的返回结果

    void pause(); //暂停线程池

    void resume(); //恢复线程池

    void shutdown(bool drain_remaining_tasks); //关闭线程池，表示是否处理剩余任务

    void recordSignal(int sig); //记录收到Linux信号

    Snapshot snapshot() const; //获取线程池状态快照

private:
    bool waitEmptySlot(); //等待任务队列空槽

    void workerLoop(std::size_t worker_id); //worker线程主循环

    void increaseRejected(); //增加被拒绝的任务数量
};

template <typename F, typename... Args>
auto AdvancedThreadPool::submit(std::string task_name, int priority, F&& f, Args&&... args)
    -> std::future<std::invoke_result_t<F, Args...>> {
    //submit是模板函数，其实现需要放在头文件中，否则容易出现链接错误。模板函数需要在编译调用点时看到完整实现    
    using ReturnType = std::invoke_result_t<F, Args...>;
    
    if (!accepting_.load() || stop_requested_.load()) {
        increaseRejected();
        throw std::runtime_error("thread pool is not accepting new tasks");
    }

    auto promise = std::make_shared<std::promise<ReturnType>>();
    /*用shared_ptr管理promise：1、submit函数在把任务推入队列后就返回了，如果promise是局部变量，当submit结束时，
    promise就会被销毁；2、任务执行是滞后的，任务可能要过几毫秒甚至几秒才被 worker 线程执行。如果 promise 已经被销毁了，
    Lambda 内部持有的引用就会变成悬空引用（Dangling Reference），导致程序崩溃（Segmentation Fault）。
    3、共享所有权：使用std::shared_ptr后，submit函数返回时，局部的promise变量销毁，但堆上的promise对象因为被lambda捕获
    而继续存活；当worker线程执行完lambda后，lambda销毁，引用计数归零，堆上的promise才会被真正释放。
    shared_ptr确保了promise的生命周期至少持续到任务执行完毕*/
    std::future<ReturnType> result = promise->get_future();

    auto bound_task = std::bind(std::forward<F>(f), std::forward<Args>(args)...);
    //std::bind把函数和参数绑定在一起，std::forward用于完美转发保留参数原本左值/右值

    TaskItem item;
    item.priority = priority;
    item.sequence = next_sequence_.fetch_add(1); //返回当前next_sequence_的值之后再将值加1
    item.name = std::move(task_name); //std::move避免拷贝
    item.job = [promise, bound_task = std::move(bound_task)]() mutable {
    //[]捕获promise和bound_task，()传入参数列表，mutable表示lambda内部可以修改捕获的变量
        try {
            if constexpr (std::is_void_v<ReturnType>) {
                bound_task(); //任务返回void，只需要调用它
                promise->set_value(); //告诉future任务已经成功执行完成，对于std::promise<void>，set_value()不需要参数
            } else {
                promise->set_value(bound_task());
                //执行任务，并将返回值保存到promise
            }
        } catch (...) {
            promise->set_exception(std::current_exception());
            throw; //重新抛异常，因为worker线程外部还会捕获异常进行统计
        }
    };

    if (!waitEmptySlot()) { //提交任务前先等待队列空槽，队列满会等待，线程池正在停止或出错，返回false
        increaseRejected(); //如果没有拿到空槽，任务被拒绝，拒绝计数加1
        throw std::runtime_error("failed to acquire queue slot");
    }

    bool queued = false; //表示任务是否已经成功入队

    try { //开始保护任务入队，如果失败需要在catch中归还信号量
        {
            std::lock_guard<std::mutex> lock(queue_mutex_); //加锁

            if (!accepting_.load() || stop_requested_.load()) { //再次检查线程池状态
                increaseRejected();
                throw std::runtime_error("thread pool stopped before task enqueue");
            }

            tasks_.push(std::move(item)); //把任务放进优先级队列，使用std::move(item)，避免不必要拷贝
            queued = true;
        }

        {
            std::unique_lock<std::shared_mutex> lock(stats_mutex_); //独占锁
            ++submitted_tasks_;
        }

        task_cv_.notify_one(); //通知一个正在等待的worker线程
    } catch (...) {
        if (!queued) { //如果没有成功入队
            sem_post(&empty_slots_); //释放之前占用的队列空槽
        }

        throw;
    }

    return result;
}

#endif
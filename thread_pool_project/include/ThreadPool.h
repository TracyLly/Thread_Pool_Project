#pragma once

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

class ThreadPool {
public:
    explicit ThreadPool(std::size_t num_threads) : stop_(false) {
        if (num_threads == 0) {
            throw std::invalid_argument("ThreadPool: num_threads must be greater than 0");
        }

        workers_.reserve(num_threads); //强制更改vector的容量至少为num_threads
        for (std::size_t i = 0; i < num_threads; ++i) {
            workers_.emplace_back(&ThreadPool::workerLoop, this);
            //动态数组workers_中存放的是std::thread,这表示创建一个新线程其入口函数是ThreadPool::workerLoop
            //this表示这个成员函数是当前线程池对象的成员函数，即每个线程启动后都会执行this->workerLoop()
        }
    }

    ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_ = true;
        }

        cv_.notify_all();

        for (std::thread &worker : workers_) {
            if (worker.joinable()) {
                worker.join();
            }
        }
    }

    ThreadPool(const ThreadPool &) = delete; //不允许拷贝
    ThreadPool &operator=(const ThreadPool &) = delete; //不允许赋值拷贝
    ThreadPool(ThreadPool &&) = delete; //不允许移动
    ThreadPool &operator=(ThreadPool &&) = delete; //不允许移动赋值

    std::size_t size() const noexcept {
        return workers_.size();
    }

    template <class F, class... Args>
    //整个线程池最关键的接口，提交一个任意可调用对象到线程池执行，并返回一个future，以后可以拿结果
    //此处用模板，希望线程池能接收各种任务：普通函数、lambda、函数对象、成员函数绑定后的对象，且参数个数不固定
    auto submit(F &&f, Args &&...args) //&&万能引用，既能接左值也能接右值，能保留原来的值类型
        -> std::future<typename std::invoke_result<F, Args...>::type> {
        //尾置返回类型。这里的type是一个依赖于模板参数的嵌套类型
        //std::invoke_result可以获取函数、成员函数和可调用对象的返回值类型
        using ReturnType = typename std::invoke_result<F, Args...>::type;

        auto task = std::make_shared<std::packaged_task<ReturnType()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...));
        //创建共享指针，指向一个无参数、返回类型为ReturnType的可调用任务
        //std::bind把一个可调用对象f、一组参数args...绑定成一个新的可调用对象
        //std::forward完美转发语法，将原来的左值/右值属性保留下去
        std::future<ReturnType> result = task->get_future();
        //task是一个std::shared_ptr<...>，在这儿访问它指向对象的成员函数
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stop_) {
                throw std::runtime_error("ThreadPool: submit on stopped pool");
            }
            tasks_.emplace([task]() {
                (*task)();
            });
            //tasks.emplace表示直接在容器内部构造元素，类型是std::function<void()>，相当于往队列放了lambda
            //[task]捕获列表，()没有参数，(*task)()表示调用这个packaged_task对象，因为packaged_task本身是可调用对象，所以可以像函数一样加()调用。
        }

        cv_.notify_one(); //唤醒一个等待线程
        return result;
    }

private:
    void workerLoop() {
        while (true) {
            std::function<void()> task;

            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [this]() {
                    return stop_ || !tasks_.empty();
                });
                //线程池停止或者有任务，就醒来继续
                if (stop_ && tasks_.empty()) {
                    return;
                }
                //线程池已经停止且没有剩余任务才退出
                task = std::move(tasks_.front());
                //因为packaged_task本身是可调用对象，所以可以像函数一样加()调用。
                tasks_.pop();
            }

            task();
            //这里执行在submit()里塞进队列的lambda
        }
    }

private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stop_;
};

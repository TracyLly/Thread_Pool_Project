#include "ThreadPool.h"

#include <chrono>
#include <exception>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {
std::mutex g_print_mutex;
//这里的锁保证多线程打印不乱
int heavyWork(int task_id, int sleep_ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));

    {
        std::lock_guard<std::mutex> lock(g_print_mutex);
        std::cout << "task " << task_id
                  << " finished in thread " << std::this_thread::get_id()
                  << ", sleep_ms=" << sleep_ms << std::endl;
    }

    return task_id * task_id;
}

void logMessage(const std::string &message, int sleep_ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));

    std::lock_guard<std::mutex> lock(g_print_mutex);
    std::cout << "log from thread " << std::this_thread::get_id()
              << ": " << message << std::endl;
}
} // namespace

int main() {
    try {
        ThreadPool pool(4);
        std::vector<std::future<int>> futures;

        std::cout << "thread pool size = " << pool.size() << std::endl;

        for (int i = 0; i < 8; ++i) {
            int sleep_ms = 300 + (i % 3) * 200;
            futures.emplace_back(pool.submit(heavyWork, i, sleep_ms));
        }
        //总共提交8个平方任务，submit提交任务，其返回std::future<int>类型的值被futures保存放到动态数组尾部
        std::future<void> log_future = pool.submit(logMessage, "hello thread pool", 400);
        //提交void日志任务，submit返回值类型std::future<void>
        std::future<int> sum_future = pool.submit([](int a, int b) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
            return a + b;
        }, 10, 32);
        //提交lambda求和任务，[]表示不捕获外部变量，(int a, int b)为参数列表，{...}函数体
        int total = 0;
        for (std::future<int> &future : futures) {
            total += future.get();//future.get()会阻塞并且只能取一次
        //因为任务是并发执行的，所以看到的task finished顺序不是按0,1,2,3...固定排列的
        }
        //开始收集8个平方任务的结果，主线程会依次等待8个heavywork任务完成，并将返回值相加
        log_future.get();

        std::cout << "sum_future result = " << sum_future.get() << std::endl;
        std::cout << "square sum total = " << total << std::endl;
        std::cout << "main thread exit" << std::endl;
    } catch (const std::exception &ex) {
        std::cerr << "error: " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}

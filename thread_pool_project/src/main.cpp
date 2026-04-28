#include "AdvancedThreadPool.hpp"
#include "Logger.hpp"
#include "Monitor.hpp"
#include "SignalWatcher.hpp"
#include "SimulatedWork.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <future>
#include <iostream>
#include <mutex>
#include <pthread.h>
#include <string>
#include <thread>
#include <vector>

int main() {
    sigset_t signal_set;
    sigemptyset(&signal_set);
    sigaddset(&signal_set, SIGINT);
    sigaddset(&signal_set, SIGTERM);

    int mask_ret = pthread_sigmask(SIG_BLOCK, &signal_set, nullptr);
    if (mask_ret != 0) {
        std::cerr << "pthread_sigmask failed" << std::endl;
        return 1;
    }

    std::atomic<bool> app_stop(false);

    AdvancedThreadPool pool(4, 10);

    std::thread signal_thread(
        signalWatcher,
        signal_set,
        std::ref(pool),
        std::ref(app_stop)
    );

    std::thread monitor_thread(
        monitorLoop,
        std::ref(pool),
        std::ref(app_stop)
    );

    std::vector<std::future<int>> futures;
    std::mutex futures_mutex;

    std::vector<std::thread> clients;

    for (int client_id = 0; client_id < 3; ++client_id) {
        clients.emplace_back([client_id, &pool, &futures, &futures_mutex, &app_stop]() {
            for (int i = 0; i < 18; ++i) {
                if (app_stop.load()) {
                    break;
                }

                int priority = i % 5;
                int cost_ms = 100 + ((client_id + i) % 6) * 80;

                std::string task_name =
                    "client-" + std::to_string(client_id) +
                    "-task-" + std::to_string(i);

                try {
                    std::future<int> future = pool.submit(
                        task_name,
                        priority,
                        simulatedWork,
                        client_id,
                        i,
                        cost_ms
                    );

                    {
                        std::lock_guard<std::mutex> lock(futures_mutex);
                        futures.push_back(std::move(future));
                    }

                    logLine(
                        "[client " + std::to_string(client_id) +
                        "] submit " + task_name +
                        ", priority=" + std::to_string(priority)
                    );
                } catch (const std::exception& e) {
                    logLine(
                        "[client " + std::to_string(client_id) +
                        "] submit failed: " + std::string(e.what())
                    );
                    break;
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(60));
            }

            logLine("[client " + std::to_string(client_id) + "] exit");
        });
    }

    std::thread admin_thread([&pool, &app_stop]() {
        std::this_thread::sleep_for(std::chrono::seconds(2));

        if (!app_stop.load()) {
            pool.pause();
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));

        if (!app_stop.load()) {
            pool.resume();
        }

        logLine("[admin] exit");
    });

    for (std::thread& client : clients) {
        if (client.joinable()) {
            client.join();
        }
    }

    if (admin_thread.joinable()) {
        admin_thread.join();
    }

    pool.shutdown(true);
    app_stop.store(true);

    if (signal_thread.joinable()) {
        signal_thread.join();
    }

    if (monitor_thread.joinable()) {
        monitor_thread.join();
    }

    int success_results = 0;
    int failed_results = 0;

    for (std::future<int>& future : futures) {
        try {
            int value = future.get();
            ++success_results;
            logLine("[main] future result=" + std::to_string(value));
        } catch (const std::exception& e) {
            ++failed_results;
            logLine("[main] future exception=" + std::string(e.what()));
        }
    }

    AdvancedThreadPool::Snapshot final_snapshot = pool.snapshot();

    logLine(
        "[main] final: submitted=" + std::to_string(final_snapshot.submitted_tasks) +
        ", completed=" + std::to_string(final_snapshot.completed_tasks) +
        ", failed=" + std::to_string(final_snapshot.failed_tasks) +
        ", rejected=" + std::to_string(final_snapshot.rejected_tasks) +
        ", success_results=" + std::to_string(success_results) +
        ", failed_results=" + std::to_string(failed_results)
    );

    logLine("[main] exit");

    return 0;
}
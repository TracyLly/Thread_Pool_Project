#include "Monitor.hpp"

#include "Logger.hpp"

#include <chrono>
#include <thread>

void monitorLoop(AdvancedThreadPool& pool, std::atomic<bool>& app_stop) {
    while (!app_stop.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(1));

        AdvancedThreadPool::Snapshot s = pool.snapshot();

        logLine(
            "[monitor] queued=" + std::to_string(s.queued_tasks) +
            ", submitted=" + std::to_string(s.submitted_tasks) +
            ", completed=" + std::to_string(s.completed_tasks) +
            ", failed=" + std::to_string(s.failed_tasks) +
            ", rejected=" + std::to_string(s.rejected_tasks) +
            ", active=" + std::to_string(s.active_workers) +
            ", signals=" + std::to_string(s.received_signals) +
            ", accepting=" + std::to_string(s.accepting) +
            ", stopping=" + std::to_string(s.stopping) +
            ", paused=" + std::to_string(s.paused)
        );

        if (s.stopping && s.queued_tasks == 0 && s.active_workers == 0) {
            break;
        }
    }

    logLine("[monitor] exit");
}
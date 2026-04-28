#include "SignalWatcher.hpp"

#include "Logger.hpp"

#include <cerrno>
#include <cstring>
#include <ctime>

void signalWatcher(
    sigset_t signal_set,
    AdvancedThreadPool& pool,
    std::atomic<bool>& app_stop
) {
    while (!app_stop.load()) {
        siginfo_t info {};
        timespec timeout {};
        timeout.tv_sec = 1;
        timeout.tv_nsec = 0;

        int sig = sigtimedwait(&signal_set, &info, &timeout);

        if (sig == -1) {
            if (errno == EAGAIN || errno == EINTR) {
                continue;
            }

            logLine(std::string("[signal] sigtimedwait failed: ") + std::strerror(errno));
            continue;
        }

        if (sig == SIGINT || sig == SIGTERM) {
            pool.recordSignal(sig);
            app_stop.store(true);

            logLine("[signal] request graceful shutdown");

            pool.shutdown(true);
            break;
        }
    }

    logLine("[signal] watcher exit");
}
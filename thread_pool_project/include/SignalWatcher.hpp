#ifndef SIGNAL_WATCHER_HPP
#define SIGNAL_WATCHER_HPP

#include "AdvancedThreadPool.hpp"

#include <atomic>
#include <csignal>

void signalWatcher(
    sigset_t signal_set,
    AdvancedThreadPool& pool,
    std::atomic<bool>& app_stop
);

#endif
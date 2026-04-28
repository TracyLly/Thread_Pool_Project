#ifndef MONITOR_HPP
#define MONITOR_HPP

#include "AdvancedThreadPool.hpp"

#include <atomic>

void monitorLoop(AdvancedThreadPool& pool, std::atomic<bool>& app_stop);

#endif
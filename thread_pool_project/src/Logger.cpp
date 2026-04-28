#include "Logger.hpp"

#include <iostream>
#include <mutex>

namespace {
std::mutex g_log_mutex;
}

void logLine(const std::string& message) {
    std::lock_guard<std::mutex> lock(g_log_mutex);
    std::cout << message << std::endl;
}
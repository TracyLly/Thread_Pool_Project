#include "SimulatedWork.hpp"

#include <chrono>
#include <stdexcept>
#include <thread>

int simulatedWork(int client_id, int task_id, int cost_ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(cost_ms));

    if ((client_id + task_id) % 17 == 0) {
        throw std::runtime_error("simulated task exception");
    }

    return client_id * 10000 + task_id;
}
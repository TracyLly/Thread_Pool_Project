#include "SignalUtils.hpp"

#include <csignal>

std::string signalName(int sig) {
    if (sig == SIGINT) {
        return "SIGINT";
    }

    if (sig == SIGTERM) {
        return "SIGTERM";
    }

    return "UNKNOWN";
}
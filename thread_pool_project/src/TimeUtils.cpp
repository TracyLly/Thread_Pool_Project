#include "TimeUtils.hpp"

timespec makeAbsTimeoutMs(long timeout_ms) {
    timespec ts {};
    clock_gettime(CLOCK_REALTIME, &ts);

    ts.tv_sec += timeout_ms / 1000;

    long extra_ns = (timeout_ms % 1000) * 1000000L;
    ts.tv_nsec += extra_ns;

    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec += ts.tv_nsec / 1000000000L;
        ts.tv_nsec %= 1000000000L;
    }

    return ts;
}
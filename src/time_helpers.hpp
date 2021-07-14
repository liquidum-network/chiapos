#ifndef SRC_CPP_TIME_HELPERS_HPP_
#define SRC_CPP_TIME_HELPERS_HPP_

#include <ctime>

inline uint64_t get_now_time_nanos(int clock = CLOCK_MONOTONIC)
{
    struct timespec spec;
    clock_gettime(clock, &spec);

    return spec.tv_sec * 1000 * 1000 * 1000 + spec.tv_nsec;
}
inline uint64_t get_coarse_now_time_nanos() { return get_now_time_nanos(CLOCK_MONOTONIC_COARSE); }

inline double get_now_time(int clock = CLOCK_MONOTONIC)
{
    return 1.0e-9 * get_now_time_nanos(clock);
}

inline uint64_t get_now_time_millis(int clock = CLOCK_MONOTONIC)
{
    struct timespec spec;
    clock_gettime(clock, &spec);

    return 1000 * spec.tv_sec + spec.tv_nsec / 1000000;
}

inline uint64_t nanoseconds(timespec ts) { return ts.tv_sec * (uint64_t)1000000000L + ts.tv_nsec; }

inline timespec nanos_to_timespec(uint64_t nanos)
{
    return timespec{
        .tv_sec = static_cast<long int>(nanos / 1000000000UL),
        .tv_nsec = static_cast<long int>(nanos % 1000000000UL),
    };
}

class TimeLogger {
public:
    TimeLogger() : start_(get_now_time(CLOCK_MONOTONIC_COARSE)) {}

    double duration() const { return get_now_time(CLOCK_MONOTONIC_COARSE) - start_; }

private:
    double start_;
};

class PausableTimeLogger {
public:
    PausableTimeLogger() {}

    double start_with_delay()
    {
        auto prev_last = last_;
        start();
        if (prev_last == 0) {
            return 0;
        }
        return 1e-9 * (last_ - prev_last);
    }

    void start() { last_ = get_now_time_nanos(); }
    void pause()
    {
        auto now = get_now_time_nanos();
        total_ += now - last_;
        last_ = now;
    }
    double total() const { return total_ * 1e-9; }

private:
    uint64_t total_ = 0;
    uint64_t last_ = 0;
};

#endif  // SRC_CPP_TIME_HELPERS_HPP_

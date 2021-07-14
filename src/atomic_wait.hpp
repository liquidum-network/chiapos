#pragma once

#include <linux/futex.h>
#include <stdint.h>
#include <sys/time.h>

#include <atomic>

#include "logging.hpp"
#include "time_helpers.hpp"

static inline int futex(
    uint32_t *uaddr,
    int futex_op,
    uint32_t val,
    const struct timespec *timeout,
    uint32_t *uaddr2,
    uint32_t val3)
{
    return syscall(SYS_futex, uaddr, futex_op, val, timeout, uaddr2, val3);
}

template <typename T>
bool atomic_wait_for(
    std::atomic<T> *object,
    T old,
    uint64_t timeout_nanos,
    std::memory_order load_order = std::memory_order_consume)
{
    auto timeout = nanos_to_timespec(timeout_nanos);

    while (true) {
        int futex_rc = futex((uint32_t *)object, FUTEX_WAIT_PRIVATE, old, &timeout, NULL, 0);
        if (futex_rc == -1 && errno == ETIMEDOUT) {
            // time has passed timeout.
            return false;
        }
        if (futex_rc == -1 && errno == EAGAIN) {
            // Value has already changed.
            SPDLOG_TRACE("futex EAGAIN");
            return true;
        }
        CHECK_EQ(futex_rc, 0, "error {}: {}", errno, ::strerror(errno));
        if (object->load(load_order) != old) {
            SPDLOG_TRACE("futex done awakening");
            return true;
        }
        SPDLOG_TRACE("futex spurioius awakening");
    }
}

template <typename T>
bool atomic_wait_until(
    std::atomic<T> *object,
    T old,
    timespec deadline,
    std::memory_order load_order = std::memory_order_consume)
{
    while (true) {
        int futex_rc = futex(
            (uint32_t *)object,
            FUTEX_WAIT_BITSET_PRIVATE,
            old,
            &deadline,
            NULL,
            FUTEX_BITSET_MATCH_ANY);
        if (futex_rc == -1 && errno == ETIMEDOUT) {
            // time has passed timeout.
            return false;
        }
        if (futex_rc == -1 && errno == EAGAIN) {
            // Value has already changed.
            SPDLOG_TRACE("futex EAGAIN");
            return true;
        }
        CHECK_EQ(futex_rc, 0, "error {}: {}", errno, ::strerror(errno));
        if (object->load(load_order) != old) {
            SPDLOG_TRACE("futex done awakening");
            return true;
        }
        SPDLOG_TRACE("futex spurioius awakening");
    }
}

template <typename T>
int atomic_notify(std::atomic<T> *object, size_t num_threads = INT_MAX)
{
    int futex_rc = futex((uint32_t *)object, FUTEX_WAKE_PRIVATE, num_threads, NULL, NULL, 0);
    CHECK_GE(futex_rc, 0);
    return futex_rc;
}

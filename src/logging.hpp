#ifndef SRC_CPP_LOGGING_HPP_
#define SRC_CPP_LOGGING_HPP_

#include "print_helpers.hpp"
#include "spdlog/spdlog.h"

#ifndef unlikely
#ifdef HAVE___BUILTIN_EXPECT
#define unlikely(x) __builtin_expect(!!(x), 0)
#define likely(x) __builtin_expect(!!(x), 1)
#else
#define unlikely(x) (x)
#define likely(x) (x)
#endif
#endif

#if NDEBUG
#define FATAL_CRASH(...) std::abort()
#else
#define FATAL_CRASH(...) throw std::runtime_error("fatal")
#endif

#ifndef SPDLOG_FATAL
#define SPDLOG_FATAL(...)                  \
    do {                                   \
        SPDLOG_CRITICAL(__VA_ARGS__);      \
        spdlog::default_logger()->flush(); \
        FATAL_CRASH();                     \
    } while (false)
#endif  // SPDLOG_FATAL

#define CHECK_OP_UNARY(condition, ...)                          \
    do {                                                        \
        const bool condition_v = (condition);                   \
        if (unlikely(!condition_v)) {                           \
            __VA_OPT__(auto msg = fmt::format(__VA_ARGS__);)    \
            SPDLOG_FATAL(                                       \
                "Check failed. {} is false" __VA_OPT__(": {}"), \
                condition_v __VA_OPT__(, std::move(msg)));      \
        }                                                       \
    } while (false)

#define CHECK_OP(op, val1, val2, ...)                        \
    do {                                                     \
        const auto val1_v = (val1);                          \
        const auto val2_v = (val2);                          \
        if (unlikely(!(val1_v op val2_v))) {                 \
            __VA_OPT__(auto msg = fmt::format(__VA_ARGS__);) \
            SPDLOG_FATAL(                                    \
                "Check failed. {} {} {}" __VA_OPT__(": {}"), \
                val1_v,                                      \
                #op,                                         \
                val2_v __VA_OPT__(, std::move(msg)));        \
        }                                                    \
    } while (false)

#define CHECK(condition, ...) CHECK_OP_UNARY(condition, __VA_ARGS__)
#define CHECK_EQ(val1, val2, ...) CHECK_OP(==, val1, val2, __VA_ARGS__)
#define CHECK_NE(val1, val2, ...) CHECK_OP(!=, val1, val2, __VA_ARGS__)
#define CHECK_LE(val1, val2, ...) CHECK_OP(<=, val1, val2, __VA_ARGS__)
#define CHECK_LT(val1, val2, ...) CHECK_OP(<, val1, val2, __VA_ARGS__)
#define CHECK_GE(val1, val2, ...) CHECK_OP(>=, val1, val2, __VA_ARGS__)
#define CHECK_GT(val1, val2, ...) CHECK_OP(>, val1, val2, __VA_ARGS__)

#if NDEBUG
#define DCHECK(...) \
    while (false) CHECK(__VA_ARGS__)
#define DCHECK_EQ(...) \
    while (false) CHECK_EQ(__VA_ARGS__)
#define DCHECK_NE(...) \
    while (false) CHECK_NE(__VA_ARGS__)
#define DCHECK_LE(...) \
    while (false) CHECK_LE(__VA_ARGS__)
#define DCHECK_LT(...) \
    while (false) CHECK_LT(__VA_ARGS__)
#define DCHECK_GE(...) \
    while (false) CHECK_GE(__VA_ARGS__)
#define DCHECK_GT(...) \
    while (false) CHECK_GT(__VA_ARGS__)
#define DCHECK_NOTNULL(...) \
    while (false) CHECK_NOTNULL(__VA_ARGS__)
#define DCHECK_STREQ(...) \
    while (false) CHECK_STREQ(__VA_ARGS__)
#define DCHECK_STRCASEEQ(...) \
    while (false) CHECK_STRCASEEQ(__VA_ARGS__)
#define DCHECK_STRNE(...) \
    while (false) CHECK_STRNE(__VA_ARGS__)
#define DCHECK_STRCASENE(...) \
    while (false) CHECK_STRCASENE(__VA_ARGS__)
#else  // NDEBUG
#define DCHECK(...) CHECK(__VA_ARGS__)
#define DCHECK_EQ(...) CHECK_EQ(__VA_ARGS__)
#define DCHECK_NE(...) CHECK_NE(__VA_ARGS__)
#define DCHECK_LE(...) CHECK_LE(__VA_ARGS__)
#define DCHECK_LT(...) CHECK_LT(__VA_ARGS__)
#define DCHECK_GE(...) CHECK_GE(__VA_ARGS__)
#define DCHECK_GT(...) CHECK_GT(__VA_ARGS__)
#define DCHECK_NOTNULL(...) CHECK_NOTNULL(__VA_ARGS__)
#define DCHECK_STREQ(...) CHECK_STREQ(__VA_ARGS__)
#define DCHECK_STRCASEEQ(...) CHECK_STRCASEEQ(__VA_ARGS__)
#define DCHECK_STRNE(...) CHECK_STRNE(__VA_ARGS__)
#define DCHECK_STRCASENE(...) CHECK_STRCASENE(__VA_ARGS__)
#endif  // NDEBUG

#ifdef NDEBUG
#define DEBUG_ONLY(X)
#else
#define DEBUG_ONLY(X) X
#endif

#endif  // SRC_CPP_LOGGING_HPP_

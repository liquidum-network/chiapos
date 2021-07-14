#ifndef SRC_CPP_PROGRESS_HPP_
#define SRC_CPP_PROGRESS_HPP_

#include <iostream>

#include "logging.hpp"

inline void progress(int phase, int64_t n, int64_t max_n)
{
    float p = (100.0 / 4) * ((phase - 1.0) + (1.0 * n / max_n));
    SPDLOG_INFO("Progress: {}", p);
}

#endif  // SRC_CPP_PROGRESS_HPP

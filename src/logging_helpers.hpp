#ifndef SRC_CPP_LOGGING_HELPERS_HPP_
#define SRC_CPP_LOGGING_HELPERS_HPP_

#include <fmt/core.h>

#include <filesystem>
#include <ostream>
#include <string_view>

#include "logging.hpp"

namespace logging {

inline void LogFileAccess(
    std::string_view path,
    std::string_view op,
    uint64_t offset,
    uint64_t length)
{
    SPDLOG_DEBUG("{} {}\t{} +\t{} (end\t{})", path, op, offset, length, offset + length);
}

inline void LogDiskAccess(
    const std::filesystem::path &path,
    std::string_view op,
    uint64_t offset,
    uint64_t length)
{
    LogFileAccess(std::string_view(path.filename().string()), op, offset, length);
}

}  // namespace logging

#endif  // SRC_CPP_LOGGING_HELPERS_HPP_

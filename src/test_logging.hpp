#pragma once

#include "logging.hpp"
#include "spdlog/pattern_formatter.h"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

inline int SetupTestLogging()
{
    auto stdout_sink = std::make_shared<spdlog::sinks::stdout_sink_mt>();
    stdout_sink->set_level(spdlog::level::info);

    auto stderr_sink = std::make_shared<spdlog::sinks::stderr_sink_mt>();
    stderr_sink->set_level(spdlog::level::err);

    std::vector<spdlog::sink_ptr> sinks = {
        std::move(stdout_sink),
        std::move(stderr_sink),
    };

    auto logger = std::make_shared<spdlog::logger>("default", begin(sinks), end(sinks));
    auto formatter = std::make_unique<spdlog::pattern_formatter>(
        "%L %Y-%m-%dT%T.%e %t %s:%#] %v", spdlog::pattern_time_type::utc);
    logger->set_formatter(std::move(formatter));
    logger->set_level(spdlog::level::trace);
    logger->flush_on(spdlog::level::info);

    spdlog::set_default_logger(std::move(logger));
    return 0;
}

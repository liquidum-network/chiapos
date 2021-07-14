#ifndef SRC_CPP_PLOTTER_HPP_
#define SRC_CPP_PLOTTER_HPP_

#ifndef _WIN32
#include <semaphore.h>
#include <sys/resource.h>
#include <unistd.h>
#endif

#include <math.h>
#include <stdio.h>

#include <iostream>

#include "nlohmann/json.hpp"
#include "pos_constants.hpp"

using json = nlohmann::json;

struct ResourceUseConfig {
    uint64_t memory_size;
    uint32_t num_buckets;
    uint32_t stripe_size;
    uint8_t num_threads;
};

void to_json(json& j, const ResourceUseConfig& p)
{
    j = json{
        {"memory_size", p.memory_size},
        {"num_buckets", p.num_buckets},
        {"stripe_size", p.stripe_size},
        {"num_threads", p.num_threads},
    };
}

void from_json(const json& j, ResourceUseConfig& p)
{
    j.at("memory_size").get_to(p.memory_size);
    j.at("num_buckets").get_to(p.num_buckets);
    j.at("stripe_size").get_to(p.stripe_size);
    j.at("num_threads").get_to(p.num_threads);
}

#endif  // SRC_CPP_PLOTTER_HPP_

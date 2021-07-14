#ifndef SRC_CPP_CHECKPOINT_HPP_
#define SRC_CPP_CHECKPOINT_HPP_

#include <optional>

#include "experiments.hpp"
#include "fmt/core.h"
#include "logging.hpp"
#include "nlohmann/json.hpp"
#include "plot_manager.hpp"
#include "serialization.hpp"

namespace checkpoint {
namespace {

std::string GetCheckpointPath(const std::vector<uint8_t>& id, uint8_t k)
{
    return fmt::format("/tmp/chia_plotter_checkpoint_id={}_k={}.json", BytesToHex(id), k);
}

}  // namespace

template <typename T>
std::optional<T> MaybeLoadFromCheckpoint(
    const std::string& checkpoint_name,
    const std::vector<uint8_t>& id,
    uint8_t k)
{
    const auto maybe_checkpoint = TryReadJsonFromFile(GetCheckpointPath(id, k));

    if (!maybe_checkpoint) {
        return {};
    }
    if (!maybe_checkpoint->contains(checkpoint_name)) {
        return {};
    }

    return (*maybe_checkpoint)[checkpoint_name].get<T>();
}

template <typename T>
void StoreCheckpoint(
    const std::string& checkpoint_name,
    const std::vector<uint8_t>& id,
    uint8_t k,
    const T& value)
{
    if (GetExperimentBool("DISABLE_CHECKPOINT") || GetExperimentBool("DISABLE_CHECKPOINT_STORE")) {
        return;
    }
    const auto checkpoint_filename = GetCheckpointPath(id, k);
    auto maybe_checkpoint = TryReadJsonFromFile(checkpoint_filename);
    auto checkpoint = maybe_checkpoint ? *std::move(maybe_checkpoint) : json{};

    json j;
    to_json(j, value);
    checkpoint[checkpoint_name] = std::move(j);

    WriteJsonToFile(checkpoint_filename, checkpoint);
    SPDLOG_INFO("Stored checkpoint {}", checkpoint_name);
}

template <typename T, typename F, typename... Args>
T Wrap(
    const std::string& checkpoint_name,
    const std::vector<uint8_t>& id,
    uint8_t k,
    F f,
    Args&&... args)
{
    if (!GetExperimentBool("DISABLE_CHECKPOINT")) {
        if (auto result = MaybeLoadFromCheckpoint<T>(checkpoint_name, id, k)) {
            SPDLOG_INFO("Loaded checkpoint {}, continuing", checkpoint_name);
            return *std::move(result);
        }
    }

    auto [result, cleanup_function] = f(std::forward<Args>(args)...);

    StoreCheckpoint(checkpoint_name, id, k, result);

    cleanup_function();

    return std::move(result);
}

}  // namespace checkpoint

#endif  // SRC_CPP_CHECKPOINT_HPP_

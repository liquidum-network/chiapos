#include "aws_async.hpp"

#include <chrono>
#include <cstdlib>

#include "logging.hpp"

namespace internal {

std::mutex await_on_shutdown_mutex_;
std::unordered_map<uint64_t, std::shared_ptr<AsyncState>> await_on_shutdown_;

std::pair<uint64_t, std::shared_ptr<AsyncState>> CreateAwaitable()
{
    uint64_t id = (static_cast<uint64_t>(RAND_MAX * rand() + rand()) << 34) +
                  (static_cast<uint64_t>(RAND_MAX * rand() + rand()) << 4) + (rand() & 0xf);

    std::scoped_lock lock(await_on_shutdown_mutex_);
    auto [iter, _] = await_on_shutdown_.try_emplace(id, std::make_shared<AsyncState>());
    return {id, iter->second};
}

void CleanupAwaitable(uint64_t id)
{
    std::scoped_lock lock(await_on_shutdown_mutex_);
    await_on_shutdown_.erase(id);
}

}  // namespace internal

void AwaitAllPendingAwaitables()
{
    using namespace internal;
    std::unique_lock<std::mutex> lock(await_on_shutdown_mutex_);

    const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(30);

    while (!await_on_shutdown_.empty()) {
        auto [_, state] = *await_on_shutdown_.begin();

        if (!state->cv.wait_until(lock, deadline, [state]() { return state->done(); })) {
            SPDLOG_ERROR(
                "timed out waiting for {} awaitables on shutdown", await_on_shutdown_.size());
            break;
        }
    }

    await_on_shutdown_.clear();
}

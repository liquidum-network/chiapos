#ifndef SRC_CPP_AWS_ASYNC_HPP_
#define SRC_CPP_AWS_ASYNC_HPP_

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

// TODO Fix includes elsewhere and remove this
#include "managed_memory.hpp"
#include "s3_utils.hpp"
#include "unique_function.hpp"

struct AsyncState : public Aws::Client::AsyncCallerContext {
    AsyncState() : done_(false) {}
    std::mutex mutex;
    std::condition_variable cv;
    std::string error_message;

    // We use unprotected references to these so they can't move.
    AsyncState(AsyncState&&) = delete;
    AsyncState(const AsyncState&) = delete;

    bool AwaitDone(int timeout_seconds = 1200)
    {
        if (done()) {
            return true;
        }
        const auto deadline =
            std::chrono::system_clock::now() + std::chrono::seconds(timeout_seconds);
        auto lock = std::unique_lock<std::mutex>(mutex);
        return cv.wait_until(lock, deadline, [this]() { return done(); });
    }

    bool done() const { return done_; }
    void set_done()
    {
        std::scoped_lock lock(mutex);
        done_ = true;
    }

    void finish(std::string err)
    {
        error_message = std::move(err);
        set_done();
        cv.notify_one();
    }

private:
    bool done_;
};

namespace internal {
extern std::mutex await_on_shutdown_mutex_;
extern std::unordered_map<uint64_t, std::shared_ptr<AsyncState>> await_on_shutdown_;

std::pair<uint64_t, std::shared_ptr<AsyncState>> CreateAwaitable();
void CleanupAwaitable(uint64_t id);

}  // namespace internal

/// TODO Fix this.
// template <typename... Args>
// auto WrapCallbackAwaitOnShutdown(unique_function<void(Args...)>&& f) -> decltype(auto)
//{
// auto [id, state] = internal::CreateAwaitable();

// return [id, f = std::move(f), state = std::move(state)](Args&&... args) {
// f(std::forward<Args>(args)...);
// state->finish("");
// internal::CleanupAwaitable(id);
//};
//}

#define WrapCallbackAwaitOnShutdown(X) X

void AwaitAllPendingAwaitables();

template <typename T>
std::future<T> create_synchronous_future(T value)
{
    std::promise<T> result_promise;
    std::future<T> future = result_promise.get_future();
    result_promise.set_value(std::move(value));
    return future;
}

#endif  // SRC_CPP_AWS_ASYNC_HPP_

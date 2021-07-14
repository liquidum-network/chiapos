#include "managed_memory.hpp"

#include <sys/mman.h>
#include <unistd.h>

#include <cstdlib>

#include "logging.hpp"
#include "time_helpers.hpp"

namespace {

static size_t max_total_size_;
static std::mutex memory_manager_mutex_;
static std::condition_variable memory_manager_cv_;

static size_t used_size_;
}  // namespace

namespace internal {
namespace {
// Need some overhead for reading past the end of buffers.
constexpr size_t kAllocExtra = 8;
constexpr size_t kMmapThreshold = 18 * 1024 * 1024;

MemoryPtr AllocateWithLockHeld(size_t size)
{
    const auto alloc_size = size + kAllocExtra;
    used_size_ += alloc_size;
    DCHECK_LE(used_size_, max_total_size_);
    SPDLOG_TRACE("Allocating {} bytes, now using {}/{}", alloc_size, used_size_, max_total_size_);

    MemoryPtr memory;
    // Use the requested size rather than alloc size to determine how to allocate.
    if (size <= kMmapThreshold) {
        memory = new uint8_t[alloc_size];
    } else {
        const auto flags = MAP_PRIVATE | MAP_ANONYMOUS;
        auto* mapped = mmap(nullptr, alloc_size, PROT_READ | PROT_WRITE, flags, -1, 0);
        CHECK_NE(mapped, MAP_FAILED);
        CHECK_NE(mapped, nullptr);
        auto madvise_result = madvise(mapped, alloc_size, MADV_WILLNEED);
        if (madvise_result != 0) {
            SPDLOG_ERROR("madvise failed {}: {}", madvise_result, ::strerror(errno));
        }
        memory = static_cast<MemoryPtr>(mapped);
    }

    return memory;
}
}  // namespace

void FreeManagedMemory(internal::MemoryPtr memory, size_t size)
{
    DCHECK_NE(memory, nullptr);
    const auto alloc_size = size + kAllocExtra;

    if (size <= kMmapThreshold) {
        delete[] memory;
    } else {
        auto unmap_result = munmap(memory, alloc_size);
        CHECK_EQ(unmap_result, 0, "errno {} {}", errno, ::strerror(errno));
    }

    std::scoped_lock lock(memory_manager_mutex_);
    used_size_ -= alloc_size;
    DCHECK_GE(used_size_, 0);
    SPDLOG_TRACE("Freed {} bytes, now using {}/{}", alloc_size, used_size_, max_total_size_);
    memory_manager_cv_.notify_all();
}

internal::MemoryPtr TryAllocateManagedMemory(
    size_t size,
    size_t required_headroom,
    uint64_t timeout_millis,
    bool required)
{
    if (required_headroom >= max_total_size_) {
        return nullptr;
    }

    TimeLogger timer;
    std::unique_lock<std::mutex> lock(memory_manager_mutex_);
    const auto this_alloc_max = max_total_size_ - required_headroom;
    if (timeout_millis == 0 && used_size_ + size <= this_alloc_max) {
        return AllocateWithLockHeld(size);
    }

    const auto deadline =
        std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_millis);

    if (memory_manager_cv_.wait_until(lock, deadline, [size, this_alloc_max, required]() {
            if (used_size_ + size <= this_alloc_max) {
                return true;
            }
            if (required) {
                SPDLOG_WARN("No memory, waiting to allocate {} bytes", size);
            } else {
                SPDLOG_DEBUG("No memory, waiting to allocate {} bytes", size);
            }
            return false;
        })) {
        auto memory = AllocateWithLockHeld(size);
        const auto duration = timer.duration();
        if (required && duration > 0.2) {
            SPDLOG_INFO("Waited {} seconds for memory", duration);
        }
        return memory;
    }
    return nullptr;
}

internal::MemoryPtr AllocateManagedMemory(size_t size, uint64_t timeout_millis)
{
    auto ptr = TryAllocateManagedMemory(size, 0, timeout_millis, true);
    if (ptr == nullptr) {
        SPDLOG_FATAL("Timed out waiting for memory of size {}", size);
    }
    return ptr;
}
}  // namespace internal

void InitMemoryManager(size_t max_total_size)
{
    max_total_size_ = max_total_size;
    used_size_ = 0;
}

#ifndef SRC_CPP_MANAGED_MEMORY_HPP_
#define SRC_CPP_MANAGED_MEMORY_HPP_

#include <atomic>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>

namespace internal {
using MemoryPtr = uint8_t*;

MemoryPtr AllocateManagedMemory(size_t size, uint64_t timeout_millis);
MemoryPtr TryAllocateManagedMemory(
    size_t size,
    size_t required_headroom,
    uint64_t timeout_millis,
    bool required = false);

void FreeManagedMemory(MemoryPtr memory, size_t size);

}  // namespace internal

void InitMemoryManager(size_t max_total_size);

class ManagedMemory {
public:
    ManagedMemory(size_t size, uint64_t timeout_millis = 1000 * 60)
        : size_(size), memory_(internal::AllocateManagedMemory(size, timeout_millis))
    {
    }

    static ManagedMemory TryAllocate(
        size_t size,
        size_t required_headroom,
        uint64_t timeout_millis = 0)
    {
        return ManagedMemory(
            size, internal::TryAllocateManagedMemory(size, required_headroom, timeout_millis));
    }

    ManagedMemory() : size_(0), memory_(nullptr) {}

    ManagedMemory(const ManagedMemory&) = delete;
    ManagedMemory& operator=(const ManagedMemory&) = delete;
    ManagedMemory(ManagedMemory&& other)
    {
        memory_ = std::move(other.memory_);
        size_ = other.size_;
        other.memory_ = nullptr;
        other.size_ = 0;
    }
    ManagedMemory& operator=(ManagedMemory&& other)
    {
        reset();
        memory_ = std::move(other.memory_);
        size_ = other.size_;
        other.memory_ = nullptr;
        other.size_ = 0;
        return *this;
    }

    ~ManagedMemory() { reset(); }

    void reset()
    {
        if (memory_) {
            internal::FreeManagedMemory(std::move(memory_), size_);
            size_ = 0;
            memory_ = nullptr;
        }
    }

    size_t size() const { return size_; }
    uint8_t* get() { return memory_; }
    const uint8_t* get() const { return memory_; }

    uint8_t* data() { return memory_; }
    const uint8_t* data() const { return memory_; }

    operator bool() const { return memory_ != nullptr; }

    bool operator==(const uint8_t* ptr) const { return memory_ == ptr; }
    bool operator!=(const uint8_t* ptr) const { return !(*this == ptr); }

private:
    ManagedMemory(size_t size, internal::MemoryPtr memory) : size_(size), memory_(std::move(memory))
    {
    }

    size_t size_;
    internal::MemoryPtr memory_;
};

#endif  // SRC_CPP_MANAGED_MEMORY_HPP_

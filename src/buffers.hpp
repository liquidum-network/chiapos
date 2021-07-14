#ifndef SRC_CPP_BUFFERS_HPP_
#define SRC_CPP_BUFFERS_HPP_

namespace buffers {

// Read buffer can be larger because we only read 1 file at a time, while we
// write to many.
constexpr size_t kTargetWriteBufferSize = 512 * 1024;
constexpr size_t kTargetReadBufferSize = 1024 * 1024;

// Needs to be much larger than the write size of ~340k.
constexpr size_t kTargetMultipartWriteBufferSize = 1024 * 1024;

template <typename T>
static std::pair<T, T> GetOverlap(T a_start, T a_end, T b_start, T b_end)
{
    if (b_start < a_end and a_start < b_end) {
        const auto overlap_start = std::max(a_start, b_start);
        const auto overlap_end = std::min(a_end, b_end);
        return {overlap_start, overlap_end};
    }
    return {0, 0};
}

inline size_t GetClosestAlignedToTarget(size_t alignment, size_t target)
{
    // Get as close to target size as possible.
    DCHECK_GT(alignment, 0);
    const auto under = (target / alignment) * alignment;
    const auto over = (target / alignment + 1) * alignment;
    return target - under < over - target ? under : over;
}

inline size_t GetLargestUnderLimit(size_t alignment, size_t size_limit)
{
    // Get as close to target size as possible.
    DCHECK_GT(alignment, 0);
    return (size_limit / alignment) * alignment;
}

}  // namespace buffers

#endif  // SRC_CPP_BUFFERS_HPP

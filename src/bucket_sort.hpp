#ifndef SRC_CPP_BUCKET_SORT_HPP_
#define SRC_CPP_BUCKET_SORT_HPP_

#include <algorithm>
#include <string>
#include <vector>

#include "logging.hpp"
#include "quicksort_entries.hpp"
#include "s3_sync_read_file.hpp"
#include "util.hpp"

#ifndef BUCKET_SORT_INPUT_FILE_TYPE
#define BUCKET_SORT_INPUT_FILE_TYPE StreamingReadFile
#endif  // BUCKET_SORT_INPUT_FILE_TYPE

namespace BucketSort {

template <size_t bucket_bits = 8>
void SortToMemory(
    BUCKET_SORT_INPUT_FILE_TYPE *input_disk,
    uint8_t *const memory,
    size_t memory_len,
    uint32_t const entry_len,
    uint64_t const num_entries,
    uint32_t const skip_bits) noexcept
{
    uint8_t *read_ptr = nullptr;

    // TODO: Need to somehow determine the bits and memory size to bound the
    // probability of overrun.
    constexpr size_t num_buckets = std::pow(2, bucket_bits);

    uint32_t bucket_ends[num_buckets];
    const uint32_t bucket_num_entries = memory_len / (num_buckets * entry_len);
    const uint32_t bucket_size_bytes = bucket_num_entries * entry_len;
    for (uint32_t bucket_idx = 0; bucket_idx < num_buckets; ++bucket_idx) {
        bucket_ends[bucket_idx] = bucket_idx * bucket_num_entries;
    }

    uint64_t num_entries_in_buf = 0;
    for (uint64_t i = 0; i < num_entries; i++) {
        if (num_entries_in_buf == 0) {
            auto [this_read_ptr, read_size] = input_disk->BorrowAligned();
            DCHECK_NE(this_read_ptr, nullptr);
            CHECK_GT(read_size, 0, "premature EOF");
            CHECK_EQ(read_size % entry_len, 0, "unaligned read");
            num_entries_in_buf = read_size / entry_len;
            read_ptr = this_read_ptr;
        }
        --num_entries_in_buf;

        const uint32_t bucket_idx =
            (*reinterpret_cast<uint64_t *>(read_ptr) << (64 - bucket_bits - skip_bits)) >>
            (64 - bucket_bits);
        DCHECK_EQ(Util::ExtractNum(read_ptr, entry_len, skip_bits, bucket_bits), bucket_idx);
        const size_t entry_pos = bucket_ends[bucket_idx]++;
        // TODO: Automatically fall back to other sort.
        // Since we already read from the file, this requires compacting the
        // already written memory, then reading the file at the end, then
        // sorting it all in place.
        CHECK_LT(
            entry_pos,
            (bucket_idx + 1) * bucket_num_entries,
            "buckets too small key={}, i={}, bucket_idx={} num_entries={} num_buckets={}",
            input_disk->GetStorageContext().local_filename,
            i,
            bucket_idx,
            num_entries,
            num_buckets);
        memcpy(memory + entry_pos * entry_len, read_ptr, entry_len);
        read_ptr += entry_len;
    }
    input_disk->Close();

    uint32_t max_bucket_entries = 0;

    // Sort each bucket.
    uint8_t *final_write_end = memory + bucket_ends[0] * entry_len;
    for (uint32_t bucket_idx = 0; bucket_idx < num_buckets; ++bucket_idx) {
        auto bucket_start_offset = bucket_idx * bucket_size_bytes;
        auto bucket_end = bucket_ends[bucket_idx] * entry_len;
        SortEntriesInPlace(
            memory + bucket_start_offset, bucket_end - bucket_start_offset, entry_len, skip_bits);
        if (bucket_idx > 0) {
            // Shift into place
            memmove(
                final_write_end, memory + bucket_start_offset, bucket_end - bucket_start_offset);
            final_write_end += bucket_end - bucket_start_offset;
        }
        max_bucket_entries =
            std::max(max_bucket_entries, (bucket_end - bucket_start_offset) / entry_len);
    }

    CHECK_EQ(final_write_end, memory + num_entries * entry_len);

    SPDLOG_TRACE(
        "largest bucket {} entries={}, max/size={:.3f}",
        input_disk->GetStorageContext().local_filename,
        max_bucket_entries,
        static_cast<double>(max_bucket_entries) / bucket_num_entries);
}

}  // namespace BucketSort
#endif  // SRC_CPP_BUCKET_SORT_HPP_

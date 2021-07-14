// Copyright 2018 Chia Network Inc

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "uniformsort.hpp"

#include <algorithm>
#include <memory>

#include "./util.hpp"
#include "logging.hpp"

namespace UniformSort {

namespace {

bool IsPositionEmpty(const uint8_t *memory, uint32_t const entry_len)
{
    for (uint32_t i = 0; i < entry_len; i++)
        if (memory[i] != 0)
            return false;
    return true;
}

template <size_t entry_len>
void SortToMemoryImpl(
    UNIFORM_SORT_INPUT_FILE_TYPE &input_disk,
    uint8_t *const memory,
    size_t memory_len,
    uint64_t const num_entries,
    uint32_t const bits_begin) noexcept
{
    struct Entry {
        std::array<uint8_t, entry_len> v;

        bool is_zero() const { return v == std::array<uint8_t, entry_len>{}; }

        bool operator>(const Entry &other) const { return v > other.v; }
    };

    uint64_t bucket_length = 0;
    // The number of buckets needed (the smallest power of 2 greater than 2 * num_entries).
    while ((1ULL << bucket_length) < 2 * num_entries) bucket_length++;
    memset(memory, 0, memory_len);

    Entry *memory_entries = reinterpret_cast<Entry *>(memory);
    size_t memory_num_entries = memory_len / entry_len;
    Entry *read_ptr = nullptr;

    uint64_t num_entries_in_buf = 0;
    for (uint64_t i = 0; i < num_entries; i++) {
        if (num_entries_in_buf == 0) {
            // Do another blocking read.
            auto [this_read_ptr, read_size] = input_disk.BorrowAligned();
            DCHECK_NE(this_read_ptr, nullptr);
            CHECK_EQ(read_size % entry_len, 0, "unaligned read");
            CHECK_GT(read_size, 0, "premature EOF");
            num_entries_in_buf = read_size / entry_len;
            read_ptr = reinterpret_cast<Entry *>(this_read_ptr);
        }
        num_entries_in_buf--;

        // First unique bits in the entry give the expected position of it in the sorted array.
        // We take 'bucket_length' bits starting with the first unique one.
        uint64_t pos = Util::ExtractNum(
            reinterpret_cast<uint8_t *>(read_ptr), entry_len, bits_begin, bucket_length);

        // As long as position is occupied by a previous entry...
        while (!memory_entries[pos].is_zero() && pos < memory_num_entries) {
            // ...store there the minimum between the two and continue to push the higher one.
            // TODO: Make sure it's safe to use > here.
            // if (memory_entries[pos] > *read_ptr) {
            if (Util::MemCmpBits(
                    memory_entries[pos].v.data(), read_ptr->v.data(), entry_len, bits_begin) > 0) {
                std::swap(memory_entries[pos], *read_ptr);
            }
            ++pos;
        }
        CHECK_LT(pos, memory_num_entries);

        // Push the entry in the first free spot.
        memory_entries[pos] = *read_ptr++;
    }
    CHECK_EQ(
        num_entries_in_buf,
        0,
        "extra {} bytes of {}",
        num_entries_in_buf * entry_len,
        num_entries * entry_len);
    input_disk.Close();

    uint64_t entries_written = 0;
    // Search the memory buffer for occupied entries.
    for (uint64_t pos = 0; entries_written < num_entries && pos < memory_num_entries; ++pos) {
        if (!memory_entries[pos].is_zero()) {
            // We've found an entry.
            // write the stored entry itself.
            memory_entries[entries_written++] = memory_entries[pos];
        }
    }

    CHECK_EQ(entries_written, num_entries, "wrote too few entries");
}

}  // namespace

void SortToMemory(
    UNIFORM_SORT_INPUT_FILE_TYPE &input_disk,
    uint8_t *const memory,
    size_t memory_len,
    uint32_t const entry_len,
    uint64_t const num_entries,
    uint32_t const bits_begin) noexcept
{
    CHECK_LE(entry_len, 27);
    if (entry_len == 27) {
        SortToMemoryImpl<27>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 26) {
        SortToMemoryImpl<26>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 25) {
        SortToMemoryImpl<25>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 24) {
        SortToMemoryImpl<24>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 23) {
        SortToMemoryImpl<23>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 22) {
        SortToMemoryImpl<22>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 21) {
        SortToMemoryImpl<21>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 20) {
        SortToMemoryImpl<20>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 19) {
        SortToMemoryImpl<19>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 18) {
        SortToMemoryImpl<18>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 17) {
        SortToMemoryImpl<17>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 16) {
        SortToMemoryImpl<16>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 15) {
        SortToMemoryImpl<15>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 14) {
        SortToMemoryImpl<14>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 13) {
        SortToMemoryImpl<13>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 12) {
        SortToMemoryImpl<12>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 11) {
        SortToMemoryImpl<11>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 10) {
        SortToMemoryImpl<10>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 9) {
        SortToMemoryImpl<9>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 8) {
        SortToMemoryImpl<8>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 7) {
        SortToMemoryImpl<7>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 6) {
        SortToMemoryImpl<6>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 5) {
        SortToMemoryImpl<5>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 4) {
        SortToMemoryImpl<4>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 3) {
        SortToMemoryImpl<3>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 2) {
        SortToMemoryImpl<2>(input_disk, memory, memory_len, num_entries, bits_begin);
    } else if (entry_len == 1) {
        SortToMemoryImpl<1>(input_disk, memory, memory_len, num_entries, bits_begin);
    }
}

void SortToMemoryDynamic(
    UNIFORM_SORT_INPUT_FILE_TYPE &input_disk,
    uint8_t *const memory,
    size_t memory_len,
    uint32_t const entry_len,
    uint64_t const num_entries,
    uint32_t const bits_begin) noexcept
{
    auto const swap_space = std::make_unique<uint8_t[]>(entry_len);
    uint8_t *read_ptr = nullptr;
    uint64_t bucket_length = 0;
    // The number of buckets needed (the smallest power of 2 greater than 2 * num_entries).
    while ((1ULL << bucket_length) < 2 * num_entries) bucket_length++;
    memset(memory, 0, memory_len);

    uint64_t num_entries_in_buf = 0;
    for (uint64_t i = 0; i < num_entries; i++) {
        if (num_entries_in_buf == 0) {
            // Do another blocking read.
            auto [this_read_ptr, read_size] = input_disk.BorrowAligned();
            DCHECK_NE(this_read_ptr, nullptr);
            CHECK_EQ(read_size % entry_len, 0, "unaligned read");
            CHECK_GT(read_size, 0, "premature EOF");
            num_entries_in_buf = read_size / entry_len;
            read_ptr = this_read_ptr;
        }
        num_entries_in_buf--;

        // First unique bits in the entry give the expected position of it in the sorted array.
        // We take 'bucket_length' bits starting with the first unique one.
        uint64_t pos = Util::ExtractNum(read_ptr, entry_len, bits_begin, bucket_length) * entry_len;

        // TODO: Maybe we can avoid the copy here and make read_ptr const by
        // keeping track of whether the current data is in swap vs read.

        // As long as position is occupied by a previous entry...
        while (!IsPositionEmpty(memory + pos, entry_len) && pos < memory_len) {
            // ...store there the minimum between the two and continue to push the higher one.
            if (Util::MemCmpBits(memory + pos, read_ptr, entry_len, bits_begin) > 0) {
                memcpy(swap_space.get(), memory + pos, entry_len);
                memcpy(memory + pos, read_ptr, entry_len);
                memcpy(read_ptr, swap_space.get(), entry_len);
            }
            pos += entry_len;
        }
        CHECK_LT(pos, memory_len);

        // Push the entry in the first free spot.
        memcpy(memory + pos, read_ptr, entry_len);
        read_ptr += entry_len;
    }
    CHECK_EQ(
        num_entries_in_buf,
        0,
        "extra {} bytes of {}",
        num_entries_in_buf * entry_len,
        num_entries * entry_len);
    input_disk.Close();

    uint64_t entries_written = 0;
    // Search the memory buffer for occupied entries.
    for (uint64_t pos = 0; entries_written < num_entries && pos < memory_len; pos += entry_len) {
        if (!IsPositionEmpty(memory + pos, entry_len)) {
            // We've found an entry.
            // write the stored entry itself.
            memcpy(memory + entries_written * entry_len, memory + pos, entry_len);
            entries_written++;
        }
    }

    CHECK_EQ(entries_written, num_entries, "wrote too few entries");
}

}  // namespace UniformSort

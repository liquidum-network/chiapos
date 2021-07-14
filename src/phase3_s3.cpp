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

#include "phase3_s3.hpp"

#include "encoding.hpp"
#include "entry_sizes.hpp"
#include "exceptions.hpp"
#include "pos_constants.hpp"
#include "readonly_sort_manager.hpp"
#include "writeonly_sort_manager.hpp"
#include "phases.hpp"
#include "progress.hpp"
#include "storage.hpp"
#include "final_file.hpp"
#include "serialization.hpp"
#include "checkpoint.hpp"
#include "nlohmann/json.hpp"
#include "time_helpers.hpp"

namespace {

using json = nlohmann::json;


struct Phase3FirstPassResult {
  std::unique_ptr<ReadOnlySortManager> R_sort_reader;
  uint64_t total_r_entries;
};

inline void to_json(json &j, const Phase3FirstPassResult &p)
{
    j = json{
        {"R_sort_reader", p.R_sort_reader},
        {"total_r_entries", p.total_r_entries},
    };
}

inline void from_json(const json &j, Phase3FirstPassResult &p)
{
    j.at("R_sort_reader").get_to(p.R_sort_reader);
    j.at("total_r_entries").get_to(p.total_r_entries);
}

struct Phase3SecondPassResult {
  std::unique_ptr<ReadOnlySortManager> L_sort_manager;
  size_t final_entries_written;
  std::vector<size_t> final_table_begin_pointers;
  FinalFile final_file;
};

inline void to_json(json &j, const Phase3SecondPassResult &p)
{
    j = json{
        {"L_sort_manager", p.L_sort_manager},
        {"final_entries_written", p.final_entries_written},
        {"final_table_begin_pointers", p.final_table_begin_pointers},
        {"final_file", p.final_file},
    };
}

inline void from_json(const json &j, Phase3SecondPassResult &p)
{
    j.at("L_sort_manager").get_to(p.L_sort_manager);
    j.at("final_entries_written").get_to(p.final_entries_written);
    j.at("final_table_begin_pointers").get_to(p.final_table_begin_pointers);
    j.at("final_file").get_to(p.final_file);
}

// This writes a number of entries into a file, in the final, optimized format. The park
// contains a checkpoint value (which is a 2k bits line point), as well as EPP (entries per
// park) entries. These entries are each divided into stub and delta section. The stub bits are
// encoded as is, but the delta bits are optimized into a variable encoding scheme. Since we
// have many entries in each park, we can approximate how much space each park with take. Format
// is: [2k bits of first_line_point]  [EPP-1 stubs] [Deltas size] [EPP-1 deltas]....
// [first_line_point] ...
void WriteParkToFile(
    FinalFile &final_file,
    uint64_t park_index,
    uint32_t park_size_bytes,
    uint128_t first_line_point,
    const std::vector<uint8_t> &park_deltas,
    const std::vector<uint64_t> &park_stubs,
    uint8_t k,
    uint8_t table_index,
    uint8_t *park_buffer,
    uint64_t const park_buffer_size)
{
    // Parks are fixed size, so we know where to start writing. The deltas will not go over
    // into the next park.
    uint8_t *index = park_buffer;

    first_line_point <<= 128 - 2 * k;
    Util::IntTo16Bytes(index, first_line_point);
    index += EntrySizes::CalculateLinePointSize(k);

    // We use ParkBits instead of Bits since it allows storing more data
    ParkBits park_stubs_bits;
    for (uint64_t stub : park_stubs) {
        park_stubs_bits.AppendValue(stub, (k - kStubMinusBits));
    }
    uint32_t stubs_size = EntrySizes::CalculateStubsSize(k);
    uint32_t stubs_valid_size = cdiv(park_stubs_bits.GetSize(), 8);
    park_stubs_bits.ToBytes(index);
    memset(index + stubs_valid_size, 0, stubs_size - stubs_valid_size);
    index += stubs_size;

    // The stubs are random so they don't need encoding. But deltas are more likely to
    // be small, so we can compress them
    double R = kRValues[table_index - 1];
    uint8_t *deltas_start = index + 2;
    size_t deltas_size = Encoding::ANSEncodeDeltas(park_deltas, R, deltas_start);

    if (!deltas_size) {
        // Uncompressed
        deltas_size = park_deltas.size();
        Util::IntToTwoBytesLE(index, deltas_size | 0x8000);
        memcpy(deltas_start, park_deltas.data(), deltas_size);
    } else {
        // Compressed
        Util::IntToTwoBytesLE(index, deltas_size);
    }

    index += 2 + deltas_size;

    CHECK_LE(index - park_buffer, park_size_bytes);
    const uint64_t offset = park_index * park_size_bytes;
    final_file.Write(IndexToTable(table_index), offset, park_buffer, index - park_buffer);
    final_file.WriteArbitrary(IndexToTable(table_index), park_size_bytes - (index - park_buffer));
}

auto RunPhase3FirstPass(
    size_t table_index,
    std::unique_ptr<ReadOnlySortManager> L_sort_manager,
    const StorageContext& storage_context,
    uint8_t k,
    Phase2Results& res2,
    uint32_t num_buckets
) -> decltype(auto) {
    uint8_t const pos_size = k;
    uint8_t const line_point_size = 2 * k - 1;

    // TODO: Until I fix checkpoints to store before deleting, I'm going to
    // delay deletes so that they come after checkpointing.
    s3_plotter::shared_thread_pool_->WaitForTasks();

    Timer computation_pass_1_timer;
    SPDLOG_INFO("Compressing tables {} and {}", table_index, table_index + 1);

    // TODO: Can delete right disk sooner here.
    ReadDisk& right_disk = res2.disk_for_table(table_index + 1);
    ReadDisk& left_disk = res2.disk_for_table(table_index);

    // Sort key is k bits for all tables. For table 7 it is just y, which
    // is k bits, and for all other tables the number of entries does not
    // exceed 0.865 * 2^k on average.
    const uint32_t right_sort_key_size = k;

    uint32_t left_entry_size_bytes = EntrySizes::GetMaxEntrySize(k, table_index, false);
    uint32_t p2_entry_size_bytes = EntrySizes::GetKeyPosOffsetSize(k);
    uint32_t right_entry_size_bytes = EntrySizes::GetMaxEntrySize(k, table_index + 1, false);
    const uint32_t new_pos_entry_size_bytes = cdiv(2 * k + ((table_index - 1) == 6 ? 1 : 0), 8);

    uint64_t left_reader = 0;
    uint64_t right_reader = 0;
    uint64_t left_reader_count = 0;
    uint64_t right_reader_count = 0;
    uint64_t total_r_entries = 0;

    // We read only from this SortManager during the second pass, so all
    // memory is available
    auto R_write_manager = std::make_unique<WriteOnlySortManager>(
        storage_context.WithSuffix(".p3.t" + std::to_string(table_index + 1)),
        num_buckets,
        right_entry_size_bytes,
        0,
        res2.table_sizes[table_index + 1],
        1.7);

    bool should_read_entry = true;
    uint64_t left_new_pos[kCachedPositionsSize];

    uint64_t old_sort_keys[kReadMinusWrite][kMaxMatchesSingleEntry];
    uint64_t old_offsets[kReadMinusWrite][kMaxMatchesSingleEntry];
    uint16_t old_counters[kReadMinusWrite] = {0};
    bool end_of_right_table = false;
    uint64_t current_pos = 0;
    uint64_t end_of_table_pos = 0;
    uint64_t greatest_pos = 0;

    uint8_t const* left_entry_disk_buf = nullptr;

    uint64_t entry_sort_key, entry_pos, entry_offset;
    uint64_t cached_entry_sort_key = 0;
    uint64_t cached_entry_pos = 0;
    uint64_t cached_entry_offset = 0;

    using LinePointBits = BitsGeneric<StackVector<2>>;

    // Similar algorithm as Backprop, to read both L and R tables simultaneously
    while (!end_of_right_table || (current_pos - end_of_table_pos <= kReadMinusWrite)) {
        old_counters[current_pos % kReadMinusWrite] = 0;

        if (end_of_right_table || current_pos <= greatest_pos) {
            while (!end_of_right_table) {
                if (should_read_entry) {
                    if (right_reader_count == res2.table_sizes[table_index + 1]) {
                        end_of_right_table = true;
                        end_of_table_pos = current_pos;
                        right_disk.FreeMemory();
                        break;
                    }
                    // The right entries are in the format from backprop, (sort_key, pos,
                    // offset)
                    uint8_t const* right_entry_buf = right_disk.BorrowAlignedAt(right_reader, p2_entry_size_bytes);
                    right_reader += p2_entry_size_bytes;
                    right_reader_count++;

                    if (right_reader_count % 200000000 == 0) {
                      SPDLOG_INFO(
                          "read {:.02f}% of {} entries",
                          right_reader_count * 100.0 / res2.table_sizes[table_index + 1],
                          res2.table_sizes[table_index + 1]);
                    }

                    entry_sort_key =
                        Util::SliceInt64FromBytes(right_entry_buf, 0, right_sort_key_size);
                    entry_pos = Util::SliceInt64FromBytes(
                        right_entry_buf, right_sort_key_size, pos_size);
                    entry_offset = Util::SliceInt64FromBytes(
                        right_entry_buf, right_sort_key_size + pos_size, kOffsetSize);
                } else if (cached_entry_pos == current_pos) {
                    entry_sort_key = cached_entry_sort_key;
                    entry_pos = cached_entry_pos;
                    entry_offset = cached_entry_offset;
                } else {
                    break;
                }

                should_read_entry = true;

                if (entry_pos + entry_offset > greatest_pos) {
                    greatest_pos = entry_pos + entry_offset;
                }
                if (entry_pos == current_pos) {
                    uint64_t const old_write_pos = entry_pos % kReadMinusWrite;
                    old_sort_keys[old_write_pos][old_counters[old_write_pos]] = entry_sort_key;
                    old_offsets[old_write_pos][old_counters[old_write_pos]] =
                        (entry_pos + entry_offset);
                    ++old_counters[old_write_pos];
                } else {
                    should_read_entry = false;
                    cached_entry_sort_key = entry_sort_key;
                    cached_entry_pos = entry_pos;
                    cached_entry_offset = entry_offset;
                    break;
                }
            }

            if (left_reader_count < res2.table_sizes[table_index]) {
                // The left entries are in the new format: (sort_key, new_pos), except for table
                // 1: (y, x).

                // TODO: unify these cases once SortManager implements
                // the ReadDisk interface
                if (table_index == 1) {
                    left_entry_disk_buf = left_disk.BorrowAlignedAt(left_reader, left_entry_size_bytes);
                    left_reader += left_entry_size_bytes;
                } else {
                    left_entry_disk_buf = L_sort_manager->ReadEntry(left_reader);
                    left_reader += new_pos_entry_size_bytes;
                }
                left_reader_count++;
            }

            // We read the "new_pos" from the L table, which for table 1 is just x. For
            // other tables, the new_pos
            if (table_index == 1) {
                // Only k bits, since this is x
                left_new_pos[current_pos % kCachedPositionsSize] =
                    Util::SliceInt64FromBytes(left_entry_disk_buf, 0, k);
            } else {
                // k+1 bits in case it overflows
                left_new_pos[current_pos % kCachedPositionsSize] =
                    Util::SliceInt64FromBytes(left_entry_disk_buf, right_sort_key_size, k);
            }
        }


        // Rewrites each right entry as (line_point, sort_key)
        if (current_pos + 1 >= kReadMinusWrite) {
            uint64_t const write_pointer_pos = current_pos - kReadMinusWrite + 1;
            uint64_t left_new_pos_1 = left_new_pos[write_pointer_pos % kCachedPositionsSize];
            for (uint32_t counter = 0;
                  counter < old_counters[write_pointer_pos % kReadMinusWrite];
                  counter++) {
                uint64_t left_new_pos_2 = left_new_pos
                    [old_offsets[write_pointer_pos % kReadMinusWrite][counter] %
                      kCachedPositionsSize];

                // A line point is an encoding of two k bit values into one 2k bit value.
                uint128_t line_point =
                    Encoding::SquareToLinePoint(left_new_pos_1, left_new_pos_2);

                DCHECK(
                    !((left_new_pos_1 > ((uint64_t)1 << k)) ||
                    (left_new_pos_2 > ((uint64_t)1 << k))),
                    "left or right positions too large {}", line_point > ((uint128_t)1 << (2 * k)));

                auto to_write = LinePointBits(line_point, line_point_size);
                to_write.AppendValue(
                    old_sort_keys[write_pointer_pos % kReadMinusWrite][counter],
                    right_sort_key_size);

                R_write_manager->AddToCacheFlushLast(to_write);
                total_r_entries++;
            }
        }
        current_pos += 1;
    }
    left_disk.Close();
    right_disk.FreeMemory();

    auto R_sort_reader = std::make_unique<ReadOnlySortManager>(
        std::move(R_write_manager), 0, SortStrategy::quicksort_last);

    computation_pass_1_timer.PrintElapsed("\tFirst computation pass time:");

    return std::make_pair(
        Phase3FirstPassResult{
          .R_sort_reader = std::move(R_sort_reader),
          .total_r_entries = total_r_entries,
        },
        [sm=std::move(L_sort_manager), &right_disk, &left_disk, table_index]() mutable {
          // Remove no longer needed file
          if (sm) {
            sm->CloseAndDelete();
          }
          if (table_index == 6) {
              right_disk.CloseAndDelete();
          }
          left_disk.CloseAndDelete();
        }
    );
}

auto RunPhase3PassSecondPass(
    size_t table_index,
    Phase3FirstPassResult first_pass_result,
    FinalFile final_file,
    std::vector<size_t> final_table_begin_pointers,
    const StorageContext& storage_context,
    uint8_t k,
    size_t right_disk_num_entries,
    uint32_t num_buckets,
    uint8_t *park_buffer,
    uint64_t const park_buffer_size
) -> decltype(auto) {
    // TODO: Until I fix checkpoints to store before deleting, I'm going to
    // delay deletes so that they come after checkpointing.
    s3_plotter::shared_thread_pool_->WaitForTasks();

    Timer computation_pass_2_timer;

    // The park size must be constant, for simplicity, but must be big enough to store EPP
    // entries. entry deltas are encoded with variable length, and thus there is no
    // guarantee that they won't override into the next park. It is only different (larger)
    // for table 1
    uint32_t park_size_bytes = EntrySizes::CalculateParkSize(k, table_index);

    // This bound seems to be tight in all cases.
    const size_t file_size_upper_bound = (
        first_pass_result.total_r_entries / kEntriesPerPark + 1) * park_size_bytes;
    final_file.OpenTable(IndexToTable(table_index), park_size_bytes, file_size_upper_bound);

    uint64_t right_reader = 0;
    size_t final_entries_written = 0;
    const uint32_t right_sort_key_size = k;
    uint32_t right_entry_size_bytes = EntrySizes::GetMaxEntrySize(k, table_index + 1, false);
    uint8_t const line_point_size = 2 * k - 1;

    // In the second pass we read from R sort manager and write to L sort
    // manager, and they both handle table (table_index + 1)'s data. The
    // newly written table consists of (sort_key, new_pos). Add one extra
    // bit for 'new_pos' to the 7-th table as it may have more than 2^k
    // entries.
    const uint32_t new_pos_entry_size_bytes = cdiv(2 * k + (table_index == 6 ? 1 : 0), 8);

    // For tables below 6 we can only use a half of memory_size since it
    // will be sorted in the first pass of the next iteration together with
    // the next table, which will use the other half of memory_size.
    // Tables 6 and 7 will be sorted alone, so we use all memory for them.
    auto L_write_manager = std::make_unique<WriteOnlySortManager>(
        storage_context.WithSuffix(".p3s.t" + std::to_string(table_index + 1)),
        num_buckets,
        new_pos_entry_size_bytes,
        0,
        right_disk_num_entries,
        1.3);

    std::vector<uint8_t> park_deltas;
    std::vector<uint64_t> park_stubs;

    park_deltas.reserve(kEntriesPerPark);
    park_stubs.reserve(kEntriesPerPark);

    uint128_t checkpoint_line_point = 0;
    uint128_t last_line_point = 0;
    uint64_t park_index = 0;

    uint8_t *right_reader_entry_buf;

    PausableTimeLogger final_write_logger;

    // Now we will write on of the final tables, since we have a table sorted by line point.
    // The final table will simply store the deltas between each line_point, in fixed space
    // groups(parks), with a checkpoint in each group.
    uint8_t const sort_key_shift = 128 - right_sort_key_size;
    uint8_t const index_shift = sort_key_shift - (k + (table_index == 6 ? 1 : 0));
    for (uint64_t index = 0; index < first_pass_result.total_r_entries; index++) {
        right_reader_entry_buf = first_pass_result.R_sort_reader->ReadEntry(right_reader);
        right_reader += right_entry_size_bytes;

        // Right entry is read as (line_point, sort_key)
        uint128_t line_point = Util::SliceInt128FromBytes(right_reader_entry_buf, 0, line_point_size);
        uint64_t sort_key =
            Util::SliceInt64FromBytes(right_reader_entry_buf, line_point_size, right_sort_key_size);

        // Write the new position (index) and the sort key
        uint128_t to_write = (uint128_t)sort_key << sort_key_shift;
        to_write |= (uint128_t)index << index_shift;

        uint8_t bytes[16];
        Util::IntTo16Bytes(bytes, to_write);
        L_write_manager->AddToCacheFlushLast(bytes);

        // Every EPP entries, writes a park
        if (index % kEntriesPerPark == 0) {
            if (index != 0) {
                auto final_write_delay = final_write_logger.start_with_delay();
                if (final_write_delay > 10.0) {
                  SPDLOG_WARN("spent {:0.2f}s between final writes, must be <20s", final_write_delay);
                }
                WriteParkToFile(
                    final_file,
                    park_index,
                    park_size_bytes,
                    checkpoint_line_point,
                    park_deltas,
                    park_stubs,
                    k,
                    table_index,
                    park_buffer,
                    park_buffer_size);
                final_write_logger.pause();

                park_index += 1;
                final_entries_written += (park_stubs.size() + 1);
            }
            park_deltas.clear();
            park_stubs.clear();

            checkpoint_line_point = line_point;
        }
        uint128_t big_delta = line_point - last_line_point;

        // Since we have approx 2^k line_points between 0 and 2^2k, the average
        // space between them when sorted, is k bits. Much more efficient than storing each
        // line point. This is diveded into the stub and delta. The stub is the least
        // significant (k-kMinusStubs) bits, and largely random/incompressible. The small
        // delta is the rest, which can be efficiently encoded since it's usually very
        // small.

        uint64_t stub = big_delta & ((1ULL << (k - kStubMinusBits)) - 1);
        uint64_t small_delta = big_delta >> (k - kStubMinusBits);

        assert(small_delta < 256);

        if ((index % kEntriesPerPark != 0)) {
            park_deltas.push_back(small_delta);
            park_stubs.push_back(stub);
        }
        last_line_point = line_point;
    }

    if (park_deltas.size() > 0) {
        // Since we don't have a perfect multiple of EPP entries, this writes the last ones
        WriteParkToFile(
            final_file,
            park_index,
            park_size_bytes,
            checkpoint_line_point,
            park_deltas,
            park_stubs,
            k,
            table_index,
            park_buffer,
            park_buffer_size);
        final_entries_written += (park_stubs.size() + 1);
    }

    computation_pass_2_timer.PrintElapsed("\tSecond computation pass time:");
    SPDLOG_DEBUG("\tWrote {} entries", final_entries_written);

    final_file.StartCloseTable(IndexToTable(table_index));

    final_table_begin_pointers[table_index + 1] =
        final_table_begin_pointers[table_index] + (park_index + 1) * park_size_bytes;
    auto L_sort_manager = std::make_unique<ReadOnlySortManager>(
        std::move(L_write_manager), 0, SortStrategy::quicksort_last, 1.4);

    // TODO: Maybe flush here.
    final_file.AwaitCloseTable(IndexToTable(table_index));

    return std::make_pair(
        Phase3SecondPassResult{
          .L_sort_manager = std::move(L_sort_manager),
          .final_entries_written = final_entries_written,
          .final_table_begin_pointers = std::move(final_table_begin_pointers),
          .final_file = std::move(final_file),
        },
        [sm=std::move(first_pass_result.R_sort_reader)]() mutable {
          if (sm) {
            sm->CloseAndDelete();
          }
        }
    );
}

}  // namespace

// Compresses the plot file tables into the final file. In order to do this, entries must be
// reorganized from the (pos, offset) bucket sorting order, to a more free line_point sorting
// order. In (pos, offset ordering), we store two pointers two the previous table, (x, y) which
// are very close together, by storing  (x, y-x), or (pos, offset), which can be done in about k
// + 8 bits, since y is in the next bucket as x. In order to decrease this, We store the actual
// entries from the previous table (e1, e2), instead of pos, offset pointers, and sort the
// entire table by (e1,e2). Then, the deltas between each (e1, e2) can be stored, which require
// around k bits.

// Converting into this format requires a few passes and sorts on disk. It also assumes that the
// backpropagation step happened, so there will be no more dropped entries. See the design
// document for more details on the algorithm.
Phase3Results RunPhase3S3(
    StorageContext storage_context,
    uint8_t k,
    Phase2Results res2,
    const std::vector<uint8_t>& id,
    uint32_t num_buckets,
    const uint8_t flags)
{
    // These variables are used in the WriteParkToFile method. They are preallocatted here
    // to save time.
    uint64_t const park_buffer_size = EntrySizes::CalculateLinePointSize(k)
        + EntrySizes::CalculateStubsSize(k) + 2
        + EntrySizes::CalculateMaxDeltasSize(k, 1);
    std::unique_ptr<uint8_t[]> park_buffer(new uint8_t[park_buffer_size]);

    Phase3SecondPassResult second_pass_result = {
      .L_sort_manager = nullptr,
      .final_entries_written = 0,
      .final_table_begin_pointers = std::vector<size_t>(12, 0),
      .final_file = FinalFile(k, BytesToHex(id)),
    };

    // Iterates through all tables, starting at 1, with L and R pointers.
    // For each table, R entries are rewritten with line points. Then, the right table is
    // sorted by line_point. After this, the right table entries are rewritten as (sort_key,
    // new_pos), where new_pos is the position in the table, where it's sorted by line_point,
    // and the line_points are written to disk to a final table. Finally, table_i is sorted by
    // sort_key. This allows us to compare to the next table.
    for (int table_index = 1; table_index < 7; table_index++) {
        Timer table_timer;
        Phase3FirstPassResult first_pass_result = checkpoint::Wrap<Phase3FirstPassResult>(
            fmt::format("p3_t{}_pass1", table_index), id, k,
            RunPhase3FirstPass,
            table_index,
            std::move(second_pass_result.L_sort_manager),
            storage_context,
            k,
            res2,
            num_buckets
        );

        second_pass_result = checkpoint::Wrap<Phase3SecondPassResult>(
            fmt::format("p3_t{}_pass2", table_index), id, k,
            RunPhase3PassSecondPass,
            table_index,
            std::move(first_pass_result),
            std::move(second_pass_result.final_file),
            std::move(second_pass_result.final_table_begin_pointers),
            storage_context,
            k,
            res2.table_sizes[table_index + 1],
            num_buckets,
            park_buffer.get(),
            park_buffer_size
        );

        table_timer.PrintElapsed("Total compress table time:");
        if (flags & SHOW_PROGRESS) { progress(3, table_index, 6); }
    }

    // No longer need table 7.
    res2.disk_for_table(7).CloseAndDelete();

    const uint32_t new_pos_entry_size_bytes = cdiv(2 * k + 1, 8);

    // These results will be used to write table P7 and the checkpoint tables in phase 4.
    return Phase3Results{
        std::move(second_pass_result.final_table_begin_pointers),
        second_pass_result.final_entries_written,
        new_pos_entry_size_bytes * 8,
        std::move(second_pass_result.L_sort_manager),
        std::move(second_pass_result.final_file),
    };
}

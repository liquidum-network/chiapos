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

#include "phase2.hpp"

#include "disk.hpp"
#include "entry_sizes.hpp"
#include "writeonly_sort_manager.hpp"
#include "readonly_sort_manager.hpp"
#include "bitfield_s3_backed.hpp"
#include "bitfield_index.hpp"
#include "progress.hpp"
#include "storage.hpp"
#include "phases.hpp"
#include "checkpoint.hpp"
#include "nlohmann/json.hpp"
#include "multipart_upload_file.hpp"
#include "readerwritercircularbuffer.h"

namespace {

using json = nlohmann::json;

struct Phase2TableResults {
  BitfieldS3Backed current_bitfield_info;

  // table 1 and 7 are special. They are passed on as plain files on disk.
  // Only table 2-6 are passed on as SortManagers, to phase3
  std::array<std::unique_ptr<ReadOnlySortManager>, 5> output_files;

  std::array<size_t, 8> table_sizes;

  FileInfo table7_info;
};

inline void to_json(json &j, const Phase2TableResults &p)
{
    j = json{
        {"current_bitfield", p.current_bitfield_info},
        {"output_files", p.output_files},
        {"table_sizes", p.table_sizes},
        {"table7_info", p.table7_info},
    };
}

inline void from_json(const json &j, Phase2TableResults &p)
{
    j.at("current_bitfield").get_to(p.current_bitfield_info);
    j.at("output_files").get_to(p.output_files);
    j.at("table_sizes").get_to(p.table_sizes);
    j.at("table7_info").get_to(p.table7_info);
}

struct Phase2Pass1Results {
  BitfieldS3Backed next_bitfield_info;
};

inline void to_json(json &j, const Phase2Pass1Results &p)
{
    j = json{
        {"next_bitfield", p.next_bitfield_info},
    };
}

inline void from_json(const json &j, Phase2Pass1Results &p)
{
    j.at("next_bitfield").get_to(p.next_bitfield_info);
}

auto RunPhase2Pass1(
    uint8_t table_index,
    const StorageContext& base_storage_context,
    const Bitfield& current_bitfield,
    size_t table_size,
    size_t max_table_size,
    const FileInfo& tmp_1_file_info,
    uint8_t const k
) -> decltype(auto) {
    // After pruning each table will have 0.865 * 2^k or fewer entries on
    // average
    uint8_t const pos_size = k;
    uint8_t const pos_offset_size = pos_size + kOffsetSize;

    Timer scan_timer;
    Bitfield next_bitfield(max_table_size);

    uint16_t const entry_size = cdiv(k + kOffsetSize + (table_index == 7 ? k : 0), 8);

    DCHECK_EQ(
        table_size * entry_size,
        tmp_1_file_info.size);
    auto first_pass_disk = StreamingReadFile(
        tmp_1_file_info.storage_context,
        table_size * entry_size,
        entry_size);

    // read_index is the number of entries we've processed so far (in the
    // current table) i.e. the index to the current entry. This is not used
    // for table 7
    moodycamel::BlockingReaderWriterCircularBuffer<uint64_t> entry_pos_offsets(128);

    std::atomic_flag done = false;
    std::atomic<size_t> total_entry_positions = 0;
    auto set_in_bitfield = [&] {
      uint64_t num_set = 0;
      bool local_done = false;
      for (size_t i = 0; i < table_size; ++i) {
        uint64_t entry_pos_offset;
        if (!local_done) {
            entry_pos_offsets.wait_dequeue(entry_pos_offset);
            local_done = done.test(std::memory_order_consume);
        }
        if (local_done && i >= total_entry_positions.load(std::memory_order_consume)) {
            break;
        }

        uint64_t entry_pos = entry_pos_offset >> kOffsetSize;
        uint64_t entry_offset = entry_pos_offset & ((1U << kOffsetSize) - 1);
        // mark the two matching entries as used (pos and pos+offset)
        next_bitfield.set(entry_pos);
        next_bitfield.set(entry_pos + entry_offset);
        num_set += 2;
      }

      CHECK_EQ(num_set, 2 * total_entry_positions);
      SPDLOG_INFO("set {}/{} bits in bitfield", num_set, next_bitfield.size());
    };
    auto bitfield_thread = std::thread(std::move(set_in_bitfield));

    if (table_index == 7) {
        // table 7 is special, we never drop anything, so just build
        // next_bitfield
        total_entry_positions.store(table_size, std::memory_order_release);
        for (size_t read_index = 0; read_index < table_size; ++read_index) {
            entry_pos_offsets.wait_enqueue(
                Util::SliceInt64FromBytes(first_pass_disk.BorrowOneAligned(), k, pos_offset_size));
        }
    } else {
        size_t total_enqueued = 0;
        for (size_t read_index = 0; read_index < table_size; ++read_index) {
            if (current_bitfield.get(read_index)) {
              const uint8_t* entry = first_pass_disk.BorrowAlignedAt(read_index * entry_size, entry_size);
              entry_pos_offsets.wait_enqueue(
                  Util::SliceInt64FromBytes(entry, 0, pos_offset_size));
              total_enqueued += 1;
            }
        }
        total_entry_positions.store(total_enqueued, std::memory_order_release);
    }

    done.test_and_set(std::memory_order_release);

    // Wake up the writer.
    entry_pos_offsets.wait_enqueue(0);

    first_pass_disk.Close();

    bitfield_thread.join();

    SPDLOG_INFO("scanned table {}", table_index);
    scan_timer.PrintElapsed("scanned time = ");

    auto next_bitfield_info = BitfieldS3Backed(
        base_storage_context.WithSuffix(fmt::format(".p2.bitfield_{}.tmp", table_index)),
        std::move(next_bitfield)
    );
    next_bitfield_info.EnsureSaved();

    return std::make_pair(
        Phase2Pass1Results{
          .next_bitfield_info = std::move(next_bitfield_info),
        },
        []{}
    );
}

auto RunPhase2OnTable(
    uint8_t table_index,
    const std::vector<uint8_t>& id,
    Phase2TableResults prev_results,
    size_t max_table_size,
    StorageContext base_storage_context,
    const FileInfo& tmp_1_file_info,
    uint8_t const k,
    uint32_t const num_buckets,
    uint8_t const flags
) -> decltype(auto) {
    SPDLOG_INFO("Backpropagating on table {}", table_index);

    // After pruning each table will have 0.865 * 2^k or fewer entries on
    // average
    uint8_t const pos_size = k;
    uint8_t const pos_offset_size = pos_size + kOffsetSize;
    uint16_t const entry_size = cdiv(k + kOffsetSize + (table_index == 7 ? k : 0), 8);
    const Bitfield& current_bitfield = prev_results.current_bitfield_info.bitfield();

    auto [next_bitfield_info] = checkpoint::Wrap<Phase2Pass1Results>(
      fmt::format("phase2_p1_table_{}", table_index), id, k,
      RunPhase2Pass1,
      table_index,
      base_storage_context,
      current_bitfield,
      prev_results.table_sizes[table_index],
      max_table_size,
      tmp_1_file_info,
      k
    );
    const Bitfield& next_bitfield = next_bitfield_info.bitfield();

    uint8_t const f7_shift = 128 - k;
    uint8_t const t7_pos_offset_shift = f7_shift - pos_offset_size;
    uint8_t const new_entry_size = EntrySizes::GetKeyPosOffsetSize(k);
    uint8_t const write_counter_shift = 128 - k;
    uint8_t const pos_offset_shift = write_counter_shift - pos_offset_size;

    SPDLOG_INFO("sorting table {}", table_index);
    Timer sort_timer;

    // read the same table again. This time we'll output it to new files:
    // * add sort_key (just the index of the current entry)
    // * update (pos, offset) to remain valid after table_index-1 has been
    //   compacted.
    // * sort by pos
    //
    // As we have to sort two adjacent tables at the same time in phase 3,
    // we can use only a half of memory_size for SortManager. However,
    // table 1 is already sorted, so we can use all memory for sorting
    // table 2.

    // Reset state.
    int64_t const table_size = prev_results.table_sizes[table_index];
    auto second_pass_disk = StreamingReadFile(
        tmp_1_file_info.storage_context,
        table_size * entry_size,
        entry_size);

    std::unique_ptr<WriteOnlySortManager> sort_manager;
    std::unique_ptr<MultipartUploadFile> t7_write_file;
    if (table_index == 7) {
      auto table_7_storage_context = tmp_1_file_info.storage_context.WithSuffix(".p2out.tmp");
      prev_results.table7_info = FileInfo{
        .storage_context=std::move(table_7_storage_context), 
        .size = second_pass_disk.GetFileSize(),
        .alignment = entry_size,
      };
      t7_write_file = std::make_unique<MultipartUploadFile>(
          MultipartUploadFile::CreateForOverwrite(prev_results.table7_info));
    } else {
      sort_manager = std::make_unique<WriteOnlySortManager>(
          base_storage_context.WithSuffix(".p2.t" + std::to_string(table_index)),
          num_buckets,
          new_entry_size,
          uint32_t(k),
          table_size,
          1.25);
    }

    // as we scan the table for the second time, we'll also need to remap
    // the positions and offsets based on the next_bitfield.
    bitfield_index const index(next_bitfield);

    int64_t write_counter = 0;
    for (int64_t read_index = 0; read_index < table_size; ++read_index)
    {
        uint8_t const* entry = second_pass_disk.BorrowOneAligned();

        uint64_t entry_f7 = 0;
        uint64_t entry_pos_offset;
        if (table_index == 7) {
            // table 7 is special, we never drop anything, so just build
            // next_bitfield
            entry_f7 = Util::SliceInt64FromBytes(entry, 0, k);
            entry_pos_offset = Util::SliceInt64FromBytes(entry, k, pos_offset_size);
        } else {
            // skipping
            if (!current_bitfield.get(read_index)) continue;

            entry_pos_offset = Util::SliceInt64FromBytes(entry, 0, pos_offset_size);
        }

        uint64_t entry_pos = entry_pos_offset >> kOffsetSize;
        uint64_t entry_offset = entry_pos_offset & ((1U << kOffsetSize) - 1);

        // assemble the new entry and write it to the sort manager

        // map the pos and offset to the new, compacted, positions and
        // offsets
        std::tie(entry_pos, entry_offset) = index.lookup(entry_pos, entry_offset);
        entry_pos_offset = (entry_pos << kOffsetSize) | entry_offset;

        uint8_t bytes[16];
        if (table_index == 7) {
            // table 7 is already sorted by pos, so we just rewrite the
            // pos and offset in-place
            uint128_t new_entry = (uint128_t)entry_f7 << f7_shift;
            new_entry |= (uint128_t)entry_pos_offset << t7_pos_offset_shift;
            Util::IntTo16Bytes(bytes, new_entry);

            // TODO: Replace this one file with two files.
            t7_write_file->Write(read_index * entry_size, bytes, entry_size);
        } else {
            // The new entry is slightly different. Metadata is dropped, to
            // save space, and the counter of the entry is written (sort_key). We
            // use this instead of (y + pos + offset) since its smaller.
            uint128_t new_entry = (uint128_t)write_counter << write_counter_shift;
            new_entry |= (uint128_t)entry_pos_offset << pos_offset_shift;
            Util::IntTo16Bytes(bytes, new_entry);

            sort_manager->AddToCacheFlushLast(bytes);
        }
        ++write_counter;
    }

    if (table_index != 7) {
        sort_timer.PrintElapsed("sort time = ");

        // clear disk caches
        prev_results.output_files[table_index - 2] = std::make_unique<ReadOnlySortManager>(
            std::move(sort_manager), 0, SortStrategy::quicksort_last);
        prev_results.table_sizes[table_index] = write_counter;
    } else if (table_index == 7) {
      t7_write_file->StartClose();
    }

    auto old_bitfield = std::move(prev_results.current_bitfield_info);

    DCHECK(!next_bitfield_info.needs_save());
    prev_results.current_bitfield_info = std::move(next_bitfield_info);

    // Make sure it's closed before deleting anything.
    if (table_index == 7) {
      t7_write_file->AwaitClose();
    }

    if (flags & SHOW_PROGRESS) {
        progress(2, 8 - table_index, 6);
    }

    return std::make_pair(
        std::move(prev_results),
        [bf=std::move(old_bitfield), d=std::move(second_pass_disk)]() mutable {
          bf.Delete();

          // The files for Table 1 and 7 are re-used, overwritten and passed on to
          // the next phase. However, table 2 through 6 are all written to sort
          // managers that are passed on to the next phase. At this point, we have
          // to delete the input files for table 2-6 to save disk space.
          // This loop doesn't cover table 1, it's handled below with the
          // FilteredDisk wrapper.
          //
          // CHANGE: Table 7 is rewritten, so we can truncate here.
          d.CloseAndDelete();
        }
    );
}

auto CalculateTable1BitfieldSize(
  Phase2TableResults prev_results,
  size_t table_size
) -> decltype(auto) {
  // lazy-compact table 1 based on current_bitfield
  //
  // at this point, table 1 still needs to be compacted, based on
  // current_bitfield. Instead of compacting it right now, defer it and read
  // from it as-if it was compacted. This saves one read and one write pass
  prev_results.table_sizes[1] = prev_results.current_bitfield_info.bitfield().count(0, table_size);
  return std::make_pair(
      std::move(prev_results),
      []{}
  );
}

}  // namespace

// Backpropagate takes in as input, a file on which forward propagation has been done.
// The purpose of backpropagate is to eliminate any dead entries that don't contribute
// to final values in f7, to minimize disk usage. A sort on disk is applied to each table,
// so that they are sorted by position.
Phase2Results RunPhase2(
    StorageContext base_storage_context,
    Phase1Result phase1_result,
    uint8_t const k,
    const std::vector<uint8_t>& id,
    uint32_t const num_buckets,
    uint8_t const flags)
{
    // Iterates through each table, starting at 6 & 7. Each iteration, we scan
    // the current table twice. In the first scan, we:

    // 1. drop entries marked as false in the current bitfield (except table 7,
    //    where we don't drop anything, this is a special case)
    // 2. mark entries in the next_bitfield that non-dropped entries have
    //    references to

    // The second scan of the table, we update the positions and offsets to
    // reflect the entries that will be dropped in the next table.
    int64_t const max_table_size = *std::max_element(
        phase1_result.table_sizes.begin(),
        phase1_result.table_sizes.end());

    Phase2TableResults prev_results = {
      .current_bitfield_info = BitfieldS3Backed(),
      .output_files = {},
      .table_sizes = to_array<8>(phase1_result.table_sizes.cbegin()),
    };

    // note that we don't iterate over table_index=1. That table is special
    // since it contains different data. We'll do an extra scan of table 1 at
    // the end, just to compact it.
    for (uint8_t table_index = 7; table_index > 1; --table_index) {
        prev_results = checkpoint::Wrap<Phase2TableResults>(
            fmt::format("phase2_table_{}", table_index), id, k,
            RunPhase2OnTable,
            table_index,
            id,
            std::move(prev_results),
            max_table_size,
            base_storage_context,
            phase1_result.read_files[table_index],
            k,
            num_buckets,
            flags);
    }

    prev_results = checkpoint::Wrap<Phase2TableResults>(
        "phase2_count_table1", id, k,
        CalculateTable1BitfieldSize,
        std::move(prev_results),
        phase1_result.table_sizes[1]);

    int16_t const entry_size = EntrySizes::GetMaxEntrySize(k, 1, false);
    DCHECK_EQ(
        static_cast<size_t>(phase1_result.table_sizes[1] * entry_size),
        phase1_result.read_files[1].size);

    SPDLOG_INFO("table 1 new size: {}", prev_results.table_sizes[1]);

    return {
        FilteredDiskInfo{
          .file_info = std::move(phase1_result.read_files[1]),
          .bitfield_info = std::move(prev_results.current_bitfield_info),
          .entry_size = entry_size,
        }
        , StreamingReadFile(std::move(prev_results.table7_info))
        , std::move(prev_results.output_files)
        , std::move(prev_results.table_sizes)
    };
}

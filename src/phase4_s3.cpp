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

#include "phase4_s3.hpp"

#include "disk.hpp"
#include "encoding.hpp"
#include "entry_sizes.hpp"
#include "phase3_s3.hpp"
#include "phases.hpp"
#include "pos_constants.hpp"
#include "util.hpp"
#include "progress.hpp"
#include "types.hpp"
#include "print_helpers.hpp"
#include "checkpoint.hpp"
#include "nlohmann/json.hpp"
#include "logging.hpp"

using json = nlohmann::json;

inline void to_json(json &j, const Phase4Results &p)
{
    j = json{
        {"final_table_begin_pointers", p.final_table_begin_pointers},
        {"final_file", p.final_file},
    };
}

inline void from_json(const json &j, Phase4Results &p)
{
    j.at("final_table_begin_pointers").get_to(p.final_table_begin_pointers);
    j.at("final_file").get_to(p.final_file);
}


namespace {

// Writes the checkpoint tables. The purpose of these tables, is to store a list of ~2^k values
// of size k (the proof of space outputs from table 7), in a way where they can be looked up for
// proofs, but also efficiently. To do this, we assume table 7 is sorted by f7, and we write the
// deltas between each f7 (which will be mostly 1s and 0s), with a variable encoding scheme
// (C3). Furthermore, we create C1 checkpoints along the way.  For example, every 10,000 f7
// entries, we can have a C1 checkpoint, and a C3 delta encoded entry with 10,000 deltas.

// Since we can't store all the checkpoints in
// memory for large plots, we create checkpoints for the checkpoints (C2), that are meant to be
// stored in memory during proving. For example, every 10,000 C1 entries, we can have a C2
// entry.

// The final table format for the checkpoints will be:
// C1 (checkpoint values)
// C2 (checkpoint values into)
// C3 (deltas of f7s between C1 checkpoints)
auto
RunPhase4S3Checkpointed(uint8_t k, uint8_t pos_size, Phase3Results phase3_result,
                        const uint8_t flags, const int max_phase4_progress_updates) -> decltype(auto)
{
    const size_t k_bytes = Util::ByteAlign(k) / 8;
    uint32_t P7_park_size = Util::ByteAlign((k + 1) * kEntriesPerPark) / 8;
    uint64_t number_of_p7_parks =
        ((phase3_result.final_entries_written == 0 ? 0 : phase3_result.final_entries_written - 1) / kEntriesPerPark) +
        1;

    const uint64_t begin_byte_C1 = phase3_result.final_table_begin_pointers[7] + number_of_p7_parks * P7_park_size;

    uint64_t total_C1_entries = cdiv(phase3_result.final_entries_written, kCheckpoint1Interval);
    const uint64_t begin_byte_C2 = begin_byte_C1 + (total_C1_entries + 1) * k_bytes;
    uint64_t total_C2_entries = cdiv(total_C1_entries, kCheckpoint2Interval);
    const uint64_t begin_byte_C3 = begin_byte_C2 + (total_C2_entries + 1) * k_bytes;

    uint32_t size_C3 = EntrySizes::CalculateC3Size(k);
    const uint64_t end_byte = begin_byte_C3 + (total_C1_entries)*size_C3;

    uint64_t plot_file_reader = 0;
    uint64_t t7_write_offset = 0;

    using C1C2Bits = BitsGeneric<StackVector<1>>;

    uint64_t prev_y = 0;
    std::vector<C1C2Bits> C1;
    std::vector<C1C2Bits> C2;
    uint64_t num_C1_entries = 0;
    std::vector<uint8_t> deltas_to_write;
    uint32_t right_entry_size_bytes = phase3_result.right_entry_size_bits / 8;

    uint8_t *right_entry_buf;
    auto C3_entry_buf = new uint8_t[size_C3]();
    auto P7_entry_buf = new uint8_t[P7_park_size]();

    SPDLOG_INFO("\tStarting to write T7 and C3 tables");

    ParkBits to_write_p7;
    const int progress_update_increment = phase3_result.final_entries_written / max_phase4_progress_updates;

    auto& final_file = phase3_result.final_file;

    final_file.OpenTable(Tables::T7, P7_park_size, number_of_p7_parks * P7_park_size);
    final_file.OpenTable(Tables::C1, k_bytes, begin_byte_C2 - begin_byte_C1);
    final_file.OpenTable(Tables::C2, k_bytes, begin_byte_C3 - begin_byte_C2);
    final_file.OpenTable(Tables::C3, size_C3, end_byte - begin_byte_C3);

    // We read each table7 entry, which is sorted by f7, but we don't need f7 anymore. Instead,
    // we will just store pos6, and the deltas in table C3, and checkpoints in tables C1 and C2.
    for (uint64_t f7_position = 0; f7_position < phase3_result.final_entries_written; f7_position++) {
        right_entry_buf = phase3_result.table7_sm->ReadEntry(plot_file_reader);

        plot_file_reader += right_entry_size_bytes;
        uint64_t entry_y = Util::SliceInt64FromBytes(right_entry_buf, 0, k);
        uint64_t entry_new_pos = Util::SliceInt64FromBytes(right_entry_buf, k, pos_size);

        if (f7_position % kEntriesPerPark == 0 && f7_position > 0) {
            //memset(P7_entry_buf, 0, P7_park_size);
            to_write_p7.ToBytes(P7_entry_buf);
            final_file.Write(Tables::T7, t7_write_offset, P7_entry_buf, P7_park_size);
            t7_write_offset += P7_park_size;
            to_write_p7.clear();
        }

        to_write_p7 += ParkBits(entry_new_pos, k + 1);

        if (f7_position % kCheckpoint1Interval == 0) {
            auto entry_y_bits = C1C2Bits(entry_y, k);
            C1.push_back(entry_y_bits);
            if (num_C1_entries > 0) {
                const size_t num_bytes =
                    Encoding::ANSEncodeDeltas(deltas_to_write, kC3R, C3_entry_buf + 2) + 2;

                // We need to be careful because deltas are variable sized, and they need to fit
                DCHECK_GT(size_C3 * 8, num_bytes);

                // Write the size
                Util::IntToTwoBytes(C3_entry_buf, num_bytes - 2);

                const size_t c3_write_offset = (num_C1_entries - 1) * size_C3;
                final_file.Write(Tables::C3, c3_write_offset, (C3_entry_buf), num_bytes);
            }
            prev_y = entry_y;
            if (f7_position % (kCheckpoint1Interval * kCheckpoint2Interval) == 0) {
                C2.emplace_back(std::move(entry_y_bits));
            }
            deltas_to_write.clear();
            ++num_C1_entries;
        } else {
            deltas_to_write.push_back(entry_y - prev_y);
            prev_y = entry_y;
        }
        if (flags & SHOW_PROGRESS && f7_position % progress_update_increment == 0) {
            progress(4, f7_position, phase3_result.final_entries_written);
        }
    }
    phase3_result.table7_sm->FreeMemory();

    // Writes the final park to disk
    //memset(P7_entry_buf, 0, P7_park_size);
    to_write_p7.ToBytes(P7_entry_buf);
    final_file.Write(Tables::T7, t7_write_offset, P7_entry_buf, P7_park_size);
    final_file.StartCloseTable(Tables::T7);

    if (!deltas_to_write.empty()) {
        const size_t num_bytes = Encoding::ANSEncodeDeltas(deltas_to_write, kC3R, C3_entry_buf + 2);
        //memset(C3_entry_buf + num_bytes + 2, 0, size_C3 - (num_bytes + 2));

        // Write the size
        Util::IntToTwoBytes(C3_entry_buf, num_bytes);

        const size_t c3_write_offset = (num_C1_entries - 1) * size_C3;
        final_file.Write(Tables::C3, c3_write_offset, (C3_entry_buf), size_C3);
    }
    final_file.StartCloseTable(Tables::C3);
    SPDLOG_INFO("\tFinished writing T1 and C3 tables");


    SPDLOG_INFO("\tWriting C1 table");
    auto C1_entry_buf = new uint8_t[k_bytes];
    uint64_t c1_write_offset = 0;
    for (const auto& c1_entry : C1) {
        c1_entry.ToBytes(C1_entry_buf);
        final_file.Write(Tables::C1, c1_write_offset, C1_entry_buf, k_bytes);
        c1_write_offset += k_bytes;
    }
    C1C2Bits(0, Util::ByteAlign(k)).ToBytes(C1_entry_buf);
    final_file.Write(Tables::C1, c1_write_offset, (C1_entry_buf), k_bytes);
    final_file.StartCloseTable(Tables::C1);
    SPDLOG_INFO("\tFinished writing C1 table");

    SPDLOG_INFO("\tWriting C2 table");
    size_t c2_write_offset = 0;
    for (auto &C2_entry : C2) {
        C2_entry.ToBytes(C1_entry_buf);
        final_file.Write(Tables::C2, c2_write_offset, C1_entry_buf, k_bytes);
        c2_write_offset += k_bytes;
    }

    // TODO: Is this just to pad the end with zeros? What is the point?
    C1C2Bits(0, Util::ByteAlign(k)).ToBytes(C1_entry_buf);
    final_file.Write(Tables::C2, c2_write_offset, (C1_entry_buf), k_bytes);
    SPDLOG_INFO("\tFinished writing C2 table");

    delete[] C3_entry_buf;
    delete[] C1_entry_buf;
    delete[] P7_entry_buf;

    final_file.AwaitCloseTable(Tables::T7);
    final_file.AwaitCloseTable(Tables::C1);
    final_file.AwaitCloseTable(Tables::C2);
    final_file.AwaitCloseTable(Tables::C3);

    phase3_result.final_table_begin_pointers[8] = begin_byte_C1;
    phase3_result.final_table_begin_pointers[9] = begin_byte_C2;
    phase3_result.final_table_begin_pointers[10] = begin_byte_C3;
    phase3_result.final_table_begin_pointers[11] = end_byte;

    return std::make_pair(
      Phase4Results{
        .final_table_begin_pointers = std::move(phase3_result.final_table_begin_pointers),
        .final_file = std::move(final_file),
      },
      [sm=std::move(phase3_result.table7_sm)]() mutable {
        sm->CloseAndDelete();
      }
    );
}

}

Phase4Results
RunPhase4S3(
    const std::vector<uint8_t>& id,
    uint8_t k, uint8_t pos_size,
    Phase3Results res, const uint8_t flags, const int max_phase4_progress_updates)
{
    auto phase4_result = checkpoint::Wrap<Phase4Results>(
            "phase4", id, k,
            RunPhase4S3Checkpointed,
            k,
            pos_size,
            std::move(res),
            flags,
            max_phase4_progress_updates);

    SPDLOG_INFO("\tFinal table pointers: {}", phase4_result.final_table_begin_pointers);
    return phase4_result;
}

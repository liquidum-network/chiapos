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

#include "phase1.hpp"

#ifndef _WIN32
#include <semaphore.h>
#include <unistd.h>
#endif

#include <math.h>
#include <stdio.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <thread>
#include <memory>
#include <mutex>

#include "chia_filesystem.hpp"

#include "calculate_bucket.hpp"
#include "entry_sizes.hpp"
#include "exceptions.hpp"
#include "pos_constants.hpp"
#include "writeonly_sort_manager.hpp"
#include "threading.hpp"
#include "util.hpp"
#include "progress.hpp"
#include "phases.hpp"
#include "serialization.hpp"
#include "plotter.hpp"
#include "readonly_sort_manager.hpp"
#include "checkpoint.hpp"
#include "nlohmann/json.hpp"
#include "multipart_upload_file.hpp"
#include "time_helpers.hpp"


#define SWITCH_ON_TABLE_INDEX(table_index, expression) \
    if ((table_index) == 1) { \
        expression <1>; \
    } else if ((table_index) == 2) { \
        expression <2>; \
    } else if ((table_index) == 3) { \
        expression <3>;\
    } else if ((table_index) == 4) {\
        expression <4>;\
    } else if ((table_index) == 5) {\
        expression <5>;\
    } else {\
        expression <6>;\
    }

namespace {

using json = nlohmann::json;

struct Phase1TableResults {
  uint64_t num_entries;
  std::unique_ptr<ReadOnlySortManager> sort_manager;
  std::array<size_t, 8> table_sizes;
  std::array<FileInfo, 8> read_files;
};


inline void to_json(json &j, const Phase1TableResults &p)
{
    j = json{
        {"num_entries", p.num_entries},
        {"sort_manager", p.sort_manager},
        {"table_sizes", p.table_sizes},
        {"read_files", p.read_files},
    };
}

inline void from_json(const json &j, Phase1TableResults &p)
{
    j.at("num_entries").get_to(p.num_entries);
    j.at("sort_manager").get_to(p.sort_manager);
    j.at("table_sizes").get_to(p.table_sizes);
    j.at("read_files").get_to(p.read_files);
}

struct PlotEntryMeta {
    uint64_t pos;
    uint128_t left_metadata;   // We only use left_metadata, unless metadata does not
    uint64_t right_metadata;   // fit in 128 bits.
    uint64_t read_posoffset;   // The combined pos and offset that this entry points to
    bool used;                 // Whether the entry was used in the next table of matches
};

struct PlotEntry {
  uint64_t y;
  PlotEntryMeta m;
};

struct GlobalData {
    uint64_t left_writer_count;
    uint64_t right_writer_count;
    uint64_t matches;
    uint64_t stripe_size;
    uint8_t num_threads;
    std::unique_ptr<ReadOnlySortManager> L_sort_manager;
    std::unique_ptr<WriteOnlySortManager> R_sort_manager;
};

struct THREADDATA {
    int index;
    Sem::type* mine;
    Sem::type* theirs;
    uint64_t right_entry_size_bytes;
    uint8_t k;
    uint64_t prevtableentries;
    uint32_t compressed_entry_size_bytes;
    MultipartUploadFile* pwrite_disk;
    MultipartUploadFile* ptable7_write_disk;
    GlobalData* globals;
};

PlotEntry GetLeftEntry(
    uint8_t const table_index,
    uint8_t const* const left_buf,
    uint8_t const k,
    uint8_t const metadata_size,
    uint8_t const pos_size)
{
    PlotEntry left_entry;
    left_entry.y = 0;
    left_entry.m.read_posoffset = 0;
    left_entry.m.left_metadata = 0;
    left_entry.m.right_metadata = 0;

    uint32_t const ysize = (table_index == 7) ? k : k + kExtraBits;

    if (table_index == 1) {
        // For table 1, we only have y and metadata
        left_entry.y = Util::SliceInt64FromBytes(left_buf, 0, k + kExtraBits);
        left_entry.m.left_metadata =
            Util::SliceInt64FromBytes(left_buf, k + kExtraBits, metadata_size);
    } else {
        // For tables 2-6, we we also have pos and offset. We need to read this because
        // this entry will be written again to the table without the y (and some entries
        // are dropped).
        left_entry.y = Util::SliceInt64FromBytes(left_buf, 0, ysize);
        left_entry.m.read_posoffset =
            Util::SliceInt64FromBytes(left_buf, ysize, pos_size + kOffsetSize);
        if (metadata_size <= 128) {
            left_entry.m.left_metadata =
                Util::SliceInt128FromBytes(left_buf, ysize + pos_size + kOffsetSize, metadata_size);
        } else {
            // Large metadatas that don't fit into 128 bits. (k > 32).
            left_entry.m.left_metadata =
                Util::SliceInt128FromBytes(left_buf, ysize + pos_size + kOffsetSize, 128);
            left_entry.m.right_metadata = Util::SliceInt64FromBytes(
                left_buf, ysize + pos_size + kOffsetSize + 128, metadata_size - 128);
        }
    }
    return left_entry;
}

template<uint8_t table_index>
void SaveNotDroppedEntries(
  const std::vector<PlotEntryMeta>& bucket_m,
  uint16_t position_map_size,
  uint16_t* R_position_map,
  uint64_t R_position_base,
  bool bStripeStartPair,
  uint8_t* left_writer_buf,
  uint8_t k,
  uint8_t pos_size,
  uint32_t compressed_entry_size_bytes,
  uint64_t left_buf_entries,
  uint64_t &stripe_left_writer_count,
  uint64_t &stripe_start_correction,
  uint64_t &left_writer_count
) {
    for (size_t bucket_index = 0; bucket_index < bucket_m.size(); bucket_index++) {
      const PlotEntryMeta& entry = bucket_m[bucket_index];
      if (!entry.used) {
        continue;
      }

      // The new position for this entry = the total amount of thing written
      // to L so far. Since we only write entries in not_dropped, about 14% of
      // entries are dropped.
      R_position_map[entry.pos % position_map_size] =
          stripe_left_writer_count - R_position_base;

      if (bStripeStartPair) {
          if (stripe_start_correction == 0xffffffffffffffff) {
              stripe_start_correction = stripe_left_writer_count;
          }

          CHECK_LT(left_writer_count, left_buf_entries, "Left writer count overrun");
          uint8_t* tmp_buf =
              left_writer_buf + left_writer_count * compressed_entry_size_bytes;

          left_writer_count++;

          // Rewrite left entry with just pos and offset, to reduce working space
          uint64_t new_left_entry;
          if (table_index == 1)
              new_left_entry = entry.left_metadata;
          else
              new_left_entry = entry.read_posoffset;
          new_left_entry <<= 64 - (table_index == 1 ? k : pos_size + kOffsetSize);
          Util::IntToEightBytes(tmp_buf, new_left_entry);
      }
      stripe_left_writer_count++;
  }
}

template<uint8_t table_index>
void* phase1_thread(const THREADDATA* ptd, GlobalData* globals_ptr)
{
    GlobalData& globals = *globals_ptr;
    uint64_t const right_entry_size_bytes = ptd->right_entry_size_bytes;
    uint8_t const k = ptd->k;
    uint8_t const pos_size = ptd->k;
    uint64_t const prevtableentries = ptd->prevtableentries;
    uint32_t const compressed_entry_size_bytes = ptd->compressed_entry_size_bytes;
    uint8_t const metadata_size = kVectorLens[table_index + 1] * k;

    uint32_t const entry_size_bytes = EntrySizes::GetMaxEntrySize(k, table_index, true);

    auto* pwrite_disk = ptd->pwrite_disk;
    auto* ptable7_write_disk = ptd->ptable7_write_disk;

    CHECK(pwrite_disk != nullptr);
    if (table_index >= 6) {
      CHECK(ptable7_write_disk != nullptr);
    }

    // Streams to read and right to tables. We will have handles to two tables. We will
    // read through the left table, compute matches, and evaluate f for matching entries,
    // writing results to the right table.
    uint64_t left_buf_entries = 5000 + (uint64_t)((1.1) * (globals.stripe_size));
    uint64_t right_buf_entries = 5000 + (uint64_t)((1.1) * (globals.stripe_size));
    std::unique_ptr<uint8_t[]> right_writer_buf(new uint8_t[right_buf_entries * right_entry_size_bytes + 7]);
    std::unique_ptr<uint8_t[]> left_writer_buf(new uint8_t[left_buf_entries * compressed_entry_size_bytes + 7]);

    FxCalculator f(k, table_index + 1);

    // Stores map of old positions to new positions (positions after dropping entries from L
    // table that did not match) Map ke
    constexpr uint16_t position_map_size = 2000;

    // Should comfortably fit 2 buckets worth of items
    uint16_t position_map_buffer[2 * position_map_size];
    uint16_t* L_position_map = &position_map_buffer[0];
    uint16_t* R_position_map = &position_map_buffer[position_map_size];

    // Start at left table pos = 0 and iterate through the whole table. Note that the left table
    // will already be sorted by y
    uint64_t totalstripes = (prevtableentries + globals.stripe_size - 1) / globals.stripe_size;
    uint64_t threadstripes = (totalstripes + globals.num_threads - 1) / globals.num_threads;

    PausableTimeLogger sem_wait_logger;
    PausableTimeLogger intermediate_write_logger;
    PausableTimeLogger final_write_logger;
    PausableTimeLogger work_logger;

    // Up to max(k + 6 + k * 4, 2 * (128 + 128))
    using EntryBits = BitsGeneric<StackVector<8>>;

    // This is a sliding window of entries, since things in bucket i can match with things in
    // bucket
    // i + 1. At the end of each bucket, we find matches between the two previous buckets.
    std::vector<uint64_t> bucket_L_y;
    std::vector<PlotEntryMeta> bucket_L_m;
    std::vector<uint64_t> bucket_R_y;
    std::vector<PlotEntryMeta> bucket_R_m;

    std::vector<std::tuple<std::pair<EntryBits, EntryBits>, uint16_t, uint16_t>>
        current_entries_to_write;
    std::vector<std::tuple<std::pair<EntryBits, EntryBits>, uint16_t, uint16_t>>
        future_entries_to_write;

    for (uint64_t stripe = 0; stripe < threadstripes; stripe++) {
        uint64_t pos = (stripe * globals.num_threads + ptd->index) * globals.stripe_size;
        uint64_t const endpos = pos + globals.stripe_size + 1;  // one y value overlap
        uint64_t left_reader = pos * entry_size_bytes;
        uint64_t left_writer_count = 0;
        uint64_t stripe_left_writer_count = 0;
        uint64_t stripe_start_correction = 0xffffffffffffffff;
        uint64_t right_writer_count = 0;
        uint64_t matches = 0;  // Total matches

        bucket_L_y.clear();
        bucket_L_m.clear();
        bucket_R_y.clear();
        bucket_R_m.clear();

        uint64_t bucket = 0;
        bool end_of_table = false;  // We finished all entries in the left table

        uint64_t ignorebucket = 0xffffffffffffffff;
        bool bMatch = false;
        bool bFirstStripeOvertimePair = false;
        bool bSecondStripOvertimePair = false;
        bool bThirdStripeOvertimePair = false;

        bool bStripePregamePair = false;
        bool bStripeStartPair = false;
        bool need_new_bucket = false;
        bool first_thread = ptd->index % globals.num_threads == 0;
        bool last_thread = ptd->index % globals.num_threads == globals.num_threads - 1;

        uint64_t L_position_base = 0;
        uint64_t R_position_base = 0;
        uint64_t newlpos = 0;
        uint64_t newrpos = 0;

        current_entries_to_write.clear();
        future_entries_to_write.clear();

        if (pos == 0) {
            bMatch = true;
            bStripePregamePair = true;
            bStripeStartPair = true;
            stripe_left_writer_count = 0;
            stripe_start_correction = 0;
        }

        Sem::Wait(ptd->theirs);

        need_new_bucket = globals.L_sort_manager->CloseToNewBucket(left_reader);
        if (need_new_bucket) {
            if (!first_thread) {
                Sem::Wait(ptd->theirs);
            }
            globals.L_sort_manager->TriggerNewBucket(left_reader);
        }
        if (!last_thread) {
            // Do not post if we are the last thread, because first thread has already
            // waited for us to finish when it starts
            Sem::Post(ptd->mine);
        }

        work_logger.start();
        while (pos < prevtableentries + 1) {
            PlotEntry left_entry = PlotEntry();
            if (pos >= prevtableentries) {
                end_of_table = true;
                left_entry.y = 0;
                left_entry.m.left_metadata = 0;
                left_entry.m.right_metadata = 0;
                left_entry.m.used = false;
            } else {
                // Reads a left entry from disk
                uint8_t* left_buf = globals.L_sort_manager->ReadEntry(left_reader);
                left_reader += entry_size_bytes;

                left_entry = GetLeftEntry(table_index, left_buf, k, metadata_size, pos_size);
            }

            // This is not the pos that was read from disk,but the position of the entry we read,
            // within L table.
            left_entry.m.pos = pos;
            left_entry.m.used = false;
            uint64_t y_bucket = left_entry.y / kBC;

            if (!bMatch) {
                if (ignorebucket == 0xffffffffffffffff) {
                    ignorebucket = y_bucket;
                } else {
                    if ((y_bucket != ignorebucket)) {
                        bucket = y_bucket;
                        bMatch = true;
                    }
                }
            }
            if (!bMatch) {
                stripe_left_writer_count++;
                R_position_base = stripe_left_writer_count;
                pos++;
                continue;
            }

            // Keep reading left entries into bucket_L and R, until we run out of things
            if (y_bucket == bucket) {
                bucket_L_y.emplace_back(left_entry.y);
                bucket_L_m.emplace_back(left_entry.m);
            } else if (y_bucket == bucket + 1) {
                bucket_R_y.emplace_back(left_entry.y);
                bucket_R_m.emplace_back(left_entry.m);
            } else {
                // SPDLOG_INFO("matching! {} and {}", bucket, bucket + 1);
                // This is reached when we have finished adding stuff to bucket_R and bucket_L,
                // so now we can compare entries in both buckets to find matches. If two entries
                // match, match, the result is written to the right table. However the writing
                // happens in the next iteration of the loop, since we need to remap positions.
                uint16_t idx_L[10000];
                uint16_t idx_R[10000];
                int32_t idx_count=0;

                if (!bucket_L_y.empty()) {
                    if (!bucket_R_y.empty()) {
                        // Compute all matches between the two buckets and save indeces.
                        idx_count = f.FindMatches(bucket_L_y, bucket_R_y, idx_L, idx_R);
                        if(idx_count >= 10000) {
                            SPDLOG_FATAL("sanity check: idx_count exceeded 10000!");
                        }
                        // We mark entries as used if they took part in a match.
                        for (int32_t i=0; i < idx_count; i++) {
                            bucket_L_m[idx_L[i]].used = true;
                            if (end_of_table) {
                                bucket_R_m[idx_R[i]].used = true;
                            }
                        }
                    }

                    // We keep maps from old positions to new positions. We only need two maps,
                    // one for L bucket and one for R bucket, and we cycle through them. Map
                    // keys are stored as positions % 2^10 for efficiency. Map values are stored
                    // as offsets from the base position for that bucket, for efficiency.
                    std::swap(L_position_map, R_position_map);
                    L_position_base = R_position_base;
                    R_position_base = stripe_left_writer_count;

                    // Adds L_bucket entries that are used to not_dropped. They are used if they
                    // either matched with something to the left (in the previous iteration), or
                    // matched with something in bucket_R (in this iteration).
                    SaveNotDroppedEntries<table_index>(
                      bucket_L_m,
                      position_map_size,
                      R_position_map,
                      R_position_base,
                      bStripeStartPair,
                      left_writer_buf.get(),
                      k,
                      pos_size,
                      compressed_entry_size_bytes,
                      left_buf_entries,
                      stripe_left_writer_count,
                      stripe_start_correction,
                      left_writer_count);
                    if (end_of_table) {
                        // In the last two buckets, we will not get a chance to enter the next
                        // iteration due to breaking from loop. Therefore to write the final
                        // bucket in this iteration, we have to add the R entries to the
                        // not_dropped list.
                        SaveNotDroppedEntries<table_index>(
                          bucket_R_m,
                          position_map_size,
                          R_position_map,
                          R_position_base,
                          bStripeStartPair,
                          left_writer_buf.get(),
                          k,
                          pos_size,
                          compressed_entry_size_bytes,
                          left_buf_entries,
                          stripe_left_writer_count,
                          stripe_start_correction,
                          left_writer_count);
                    }

                    // Two vectors to keep track of things from previous iteration and from this
                    // iteration.
                    current_entries_to_write = std::move(future_entries_to_write);

                    for (int32_t i=0; i < idx_count; i++) {
                        uint64_t L_entry_y = bucket_L_y[idx_L[i]];
                        PlotEntryMeta& L_entry_m = bucket_L_m[idx_L[i]];
                        PlotEntryMeta& R_entry_m = bucket_R_m[idx_R[i]];

                        if (bStripeStartPair)
                            matches++;

                        // Sets the R entry to used so that we don't drop in next iteration
                        R_entry_m.used = true;
                        // Computes the output pair (fx, new_metadata)
                        if (metadata_size <= 128) {
                            const std::pair<EntryBits, EntryBits>& f_output = f.CalculateBucket(
                                EntryBits(L_entry_y, k + kExtraBits),
                                EntryBits(L_entry_m.left_metadata, metadata_size),
                                EntryBits(R_entry_m.left_metadata, metadata_size));
                            future_entries_to_write.emplace_back(
                                f_output,
                                L_entry_m.pos % position_map_size,
                                R_entry_m.pos % position_map_size);
                        } else {
                            // Metadata does not fit into 128 bits
                            const std::pair<EntryBits, EntryBits>& f_output = f.CalculateBucket(
                                EntryBits(L_entry_y, k + kExtraBits),
                                EntryBits(L_entry_m.left_metadata, 128) +
                                    EntryBits(L_entry_m.right_metadata, metadata_size - 128),
                                EntryBits(R_entry_m.left_metadata, 128) +
                                    EntryBits(R_entry_m.right_metadata, metadata_size - 128));
                            future_entries_to_write.emplace_back(
                                f_output,
                                L_entry_m.pos % position_map_size,
                                R_entry_m.pos % position_map_size);
                        }
                    }

                    // At this point, future_entries_to_write contains the matches of buckets L
                    // and R, and current_entries_to_write contains the matches of L and the
                    // bucket left of L. These are the ones that we will write.
                    uint16_t final_current_entry_size = current_entries_to_write.size();
                    if (end_of_table) {
                        // For the final bucket, write the future entries now as well, since we
                        // will break from loop
                        current_entries_to_write.insert(
                            current_entries_to_write.end(),
                            future_entries_to_write.begin(),
                            future_entries_to_write.end());
                    }
                    for (size_t i = 0; i < current_entries_to_write.size(); i++) {
                        const auto& [f_output, L_entry_pos_in_map, R_entry_pos_in_map] = current_entries_to_write[i];

                        // We only need k instead of k + kExtraBits bits for the last table
                        EntryBits new_entry = table_index + 1 == 7 ? std::get<0>(f_output).Slice(0, k)
                                                              : std::get<0>(f_output);

                        // Maps the new positions. If we hit end of pos, we must write things in
                        // both final_entries to write and current_entries_to_write, which are
                        // in both position maps.
                        if (!end_of_table || i < final_current_entry_size) {
                            newlpos =
                                L_position_map[L_entry_pos_in_map] + L_position_base;
                        } else {
                            newlpos =
                                R_position_map[L_entry_pos_in_map] + R_position_base;
                        }
                        newrpos = R_position_map[R_entry_pos_in_map] + R_position_base;
                        // Position in the previous table
                        new_entry.AppendValue(newlpos, pos_size);

                        // Offset for matching entry
                        CHECK_LE(
                            newrpos - newlpos,
                            (1U << kOffsetSize) * 97 / 100,
                            "Offset too large: " + std::to_string(newrpos - newlpos));

                        new_entry.AppendValue(newrpos - newlpos, kOffsetSize);
                        // New metadata which will be used to compute the next f
                        new_entry += std::get<1>(f_output);

                        CHECK_LT(right_writer_count, right_buf_entries, "Left writer count overrun");

                        if (bStripeStartPair) {
                            uint8_t* right_buf =
                                right_writer_buf.get() + right_writer_count * right_entry_size_bytes;
                            new_entry.ToBytes(right_buf);
                            right_writer_count++;
                        }
                    }
                }

                if (pos >= endpos) {
                    if (!bFirstStripeOvertimePair)
                        bFirstStripeOvertimePair = true;
                    else if (!bSecondStripOvertimePair)
                        bSecondStripOvertimePair = true;
                    else if (!bThirdStripeOvertimePair)
                        bThirdStripeOvertimePair = true;
                    else {
                        break;
                    }
                } else {
                    if (!bStripePregamePair)
                        bStripePregamePair = true;
                    else if (!bStripeStartPair)
                        bStripeStartPair = true;
                }

                if (y_bucket == bucket + 2) {
                    // We saw a bucket that is 2 more than the current, so we just set L = R, and R
                    // = [entry]
                    bucket_L_y = std::move(bucket_R_y);
                    bucket_L_m = std::move(bucket_R_m);
                    bucket_R_y.emplace_back(std::move(left_entry.y));
                    bucket_R_m.emplace_back(std::move(left_entry.m));
                    ++bucket;
                } else {
                    // We saw a bucket that >2 more than the current, so we just set L = [entry],
                    // and R = []
                    bucket = y_bucket;
                    bucket_L_y.clear();
                    bucket_L_m.clear();
                    bucket_L_y.emplace_back(std::move(left_entry.y));
                    bucket_L_m.emplace_back(std::move(left_entry.m));
                    bucket_R_y.clear();
                    bucket_R_m.clear();
                }
            }
            // Increase the read pointer in the left table, by one
            ++pos;
        }
        work_logger.pause();

        // If we needed new bucket, we already waited
        // Do not wait if we are the first thread, since we are guaranteed that everything is written
        if (!need_new_bucket && !first_thread) {
            Sem::Wait(ptd->theirs);
        }

        uint32_t const ysize = (table_index + 1 == 7) ? k : k + kExtraBits;
        uint32_t const startbyte = ysize / 8;
        uint32_t const endbyte = (ysize + pos_size + 7) / 8 - 1;
        uint64_t const shiftamt = (8 - ((ysize + pos_size) % 8)) % 8;
        uint64_t const correction = (globals.left_writer_count - stripe_start_correction) << shiftamt;

        // Correct positions
        for (uint32_t i = 0; i < right_writer_count; i++) {
            uint64_t posaccum = 0;
            uint8_t* entrybuf = right_writer_buf.get() + i * right_entry_size_bytes;

            for (uint32_t j = startbyte; j <= endbyte; j++) {
                posaccum = (posaccum << 8) | (entrybuf[j]);
            }
            posaccum += correction;
            for (uint32_t j = endbyte; j >= startbyte; --j) {
                entrybuf[j] = posaccum & 0xff;
                posaccum = posaccum >> 8;
            }
        }

        auto intermediate_write_delay = intermediate_write_logger.start_with_delay();
        if (intermediate_write_delay > 10.0) {
          SPDLOG_WARN("T{} spent {:0.2f}s between intermediate writes, must be <20s",
              ptd->index, intermediate_write_delay);
        }
        if (table_index < 6) {
            for (uint64_t i = 0; i < right_writer_count; i++) {
                globals.R_sort_manager->AddToCache(right_writer_buf.get() + i * right_entry_size_bytes);
            }
        } else {
            // Writes out the right table for table 7
            ptable7_write_disk->WriteAppend(
                right_writer_buf.get(),
                right_writer_count * right_entry_size_bytes);
            ptable7_write_disk->Flush();
        }
        intermediate_write_logger.pause();

        globals.right_writer_count += right_writer_count;

        auto final_write_delay = final_write_logger.start_with_delay();
        if (final_write_delay > 10.0) {
          SPDLOG_WARN("T{} spent {:0.2f}s between final writes, must be <20s", ptd->index, final_write_delay);
        }
        pwrite_disk->WriteAppend(
            left_writer_buf.get(), left_writer_count * compressed_entry_size_bytes);
        pwrite_disk->Flush();
        final_write_logger.pause();

        globals.left_writer_count += left_writer_count;

        globals.matches += matches;
        Sem::Post(ptd->mine);

        if (stripe % 3200 == 0) {
          SPDLOG_INFO(
              "T{} done {:.02f}% of {} stripes",
              ptd->index, stripe * 100.0 / threadstripes, threadstripes);
        }
    }

    SPDLOG_INFO(
        "T{} spent {:.02f}s working, {:.02f}s writing intermediates, {:.02f}s writing final",
        ptd->index,
        work_logger.total(),
        intermediate_write_logger.total(),
        final_write_logger.total()
    );
    return 0;
}

void* F1SingleThread(int const index, uint8_t const k, const uint8_t* id,
    WriteOnlySortManager* R_sort_manager
) {
    uint32_t constexpr entry_size_bytes = 16;
    uint64_t const max_value = ((uint64_t)1 << (k));

    uint64_t f1_entries[1U << kBatchSizes];

    F1Calculator f1(k, id);
    std::unique_ptr<uint8_t[]> right_writer_buf(new uint8_t[entry_size_bytes]);

    // Instead of computing f1(1), f1(2), etc, for each x, we compute them in batches
    // to increase CPU efficency.
    for (uint64_t lp = index; lp <= (((uint64_t)1) << (k - kBatchSizes)); ++lp)
    {
        // For each pair x, y in the batch
        uint64_t x = lp * (1 << (kBatchSizes));

        uint64_t const loopcount = std::min(max_value - x, (uint64_t)1 << (kBatchSizes));

        // Instead of computing f1(1), f1(2), etc, for each x, we compute them in batches
        // to increase CPU efficency.
        f1.CalculateBuckets(x, loopcount, f1_entries);
        for (uint32_t i = 0; i < loopcount; i++) {
            uint128_t entry = (uint128_t)f1_entries[i] << (128 - kExtraBits - k);
            entry |= (uint128_t)x << (128 - kExtraBits - 2 * k);
            Util::IntTo16Bytes(right_writer_buf.get(), entry);
            R_sort_manager->AddToCache(right_writer_buf.get());
            x++;
        }
    }

    return 0;
}

auto RunPhase1OnTable(
  uint8_t table_index,
  Phase1TableResults prev_table_results,
  StorageContext storage_context,
  uint8_t const k,
  const uint8_t* const id,
  uint32_t const num_buckets,
  uint32_t const stripe_size,
  uint8_t const num_threads,
  uint8_t const flags
) -> decltype(auto) {
    // TODO: Until I fix checkpoints to store before deleting, I'm going to
    // delay deletes so that they come after checkpointing.
    s3_plotter::shared_thread_pool_->WaitForTasks();

    Timer table_timer;

    // Determines how many bytes the entries in our left and right tables will take up.
    uint32_t const entry_size_bytes = EntrySizes::GetMaxEntrySize(k, table_index, true);
    uint32_t compressed_entry_size_bytes = EntrySizes::GetMaxEntrySize(k, table_index, false);
    uint32_t right_entry_size_bytes = EntrySizes::GetMaxEntrySize(k, table_index + 1, true);

    if (flags & ENABLE_BITFIELD && table_index != 1) {
        // We only write pos and offset to tables 2-6 after removing
        // metadata
        compressed_entry_size_bytes = cdiv(k + kOffsetSize, 8);
        if (table_index == 6) {
            // Table 7 will contain f7, pos and offset
            right_entry_size_bytes = EntrySizes::GetKeyPosOffsetSize(k);
        }
    }

    SPDLOG_INFO("Computing table {}", int{table_index + 1});
    // Start of parallel execution

    FxCalculator f(k, table_index + 1);  // dummy to load static table
    const auto prevtableentries = prev_table_results.num_entries;

    const double sort_manager_size_factor = k < 32 ? 1.05 : 0.97;
    GlobalData globals = {
      .left_writer_count = 0,
      .right_writer_count = 0,
      .matches = 0,
      .stripe_size = stripe_size,
      .num_threads = num_threads,
      .L_sort_manager = std::move(prev_table_results.sort_manager),

      // TODO: Looks like we only drop entires here, so "prevtableentries"
      // should be an upper bound on the number of entries, but I need to
      // confirm.
      .R_sort_manager = std::make_unique<WriteOnlySortManager>(
          storage_context.WithSuffix(".p1.t" + std::to_string(table_index + 1)),
          num_buckets,
          right_entry_size_bytes,
          0,
          prevtableentries,
          sort_manager_size_factor),
    };

    globals.L_sort_manager->TriggerNewBucket(0);

    Timer computation_pass_timer;

    const double final_table_size_factor = k < 32 ? 1.0 : 0.87;
    auto write_disk = MultipartUploadFile(
        storage_context.WithSuffix(".table" + std::to_string(table_index) + ".tmp"),
        compressed_entry_size_bytes,
        // TODO: These bounds can probably be tighter.
        static_cast<size_t>(final_table_size_factor * compressed_entry_size_bytes * prevtableentries)
    );
    MultipartUploadFile table7_write_disk;
    if (table_index == 6) {
      const double table_6_size_factor = k < 32 ? 0.62 : 0.57;
      table7_write_disk = MultipartUploadFile(
          storage_context.WithSuffix(".table7.tmp"),
          entry_size_bytes,
          static_cast<size_t>(table_6_size_factor * entry_size_bytes * prevtableentries)
      );
    }

    auto td = std::make_unique<THREADDATA[]>(num_threads);
    auto mutex = std::make_unique<Sem::type[]>(num_threads);

    std::vector<std::thread> threads;

    for (int i = 0; i < num_threads; i++) {
        mutex[i] = Sem::Create();
    }

    for (int i = 0; i < num_threads; i++) {
        td[i].index = i;
        td[i].mine = &mutex[i];
        td[i].theirs = &mutex[(num_threads + i - 1) % num_threads];

        td[i].prevtableentries = prevtableentries;
        td[i].right_entry_size_bytes = right_entry_size_bytes;
        td[i].k = k;
        td[i].compressed_entry_size_bytes = compressed_entry_size_bytes;
        td[i].pwrite_disk = &write_disk;
        td[i].ptable7_write_disk = &table7_write_disk;
    }

    void* (*phase1_thread_function)(const THREADDATA*, GlobalData*);
    SWITCH_ON_TABLE_INDEX(table_index, phase1_thread_function = phase1_thread);

    for (int i = 1; i < num_threads; i++) {
        threads.emplace_back(phase1_thread_function, &td[i], &globals);
    }
    Sem::Post(&mutex[num_threads - 1]);

    // Run thread 0 synchronously
    phase1_thread_function(&td[0], &globals);
    for (auto& t : threads) {
        t.join();
    }

    for (int i = 0; i < num_threads; i++) {
        Sem::Destroy(mutex[i]);
    }

    // end of parallel execution

    // Total matches found in the left table
    SPDLOG_DEBUG("\tTotal matches: {}", globals.matches);

    prev_table_results.table_sizes[table_index] = globals.left_writer_count;
    prev_table_results.table_sizes[table_index + 1] = globals.right_writer_count;

    // Truncates the file after the final write position, deleting no longer useful
    // working space
    write_disk.LogSizeStats();
    write_disk.StartClose();

    prev_table_results.read_files[table_index] = write_disk.GetFileInfo();

    if (table_index < 6) {
        prev_table_results.sort_manager = std::make_unique<ReadOnlySortManager>(
            std::move(globals.R_sort_manager), stripe_size);
    } else {
        table7_write_disk.LogSizeStats();
        table7_write_disk.StartClose();
        prev_table_results.read_files[table_index + 1] = table7_write_disk.GetFileInfo();
    }

    // Resets variables
    if (globals.matches != globals.right_writer_count) {
        throw InvalidStateException(
            "Matches do not match with number of write entries " +
            std::to_string(globals.matches) + " " + std::to_string(globals.right_writer_count));
    }

    write_disk.AwaitClose();
    if (table_index == 6) {
      table7_write_disk.AwaitClose();
    }

    table_timer.PrintElapsed("Forward propagation table time:");
    if (flags & SHOW_PROGRESS) {
        progress(1, table_index, 6);
    }

    prev_table_results.num_entries = globals.right_writer_count;
    return std::make_pair(
      std::move(prev_table_results),
      [sm=std::move(globals.L_sort_manager)]() mutable { sm->CloseAndDelete(); }
    );
}

auto RunPhase1Table0(
    StorageContext storage_context,
    uint8_t const k,
    const std::vector<uint8_t>& id,
    uint32_t const num_buckets,
    uint32_t const stripe_size,
    uint8_t const num_threads
) -> decltype(auto) {
    SPDLOG_INFO("Computing table 1");
    Timer f1_start_time;

    //xxx write the buckets in sorted order initially? Skip the later sort step
    uint32_t const t1_entry_size_bytes = EntrySizes::GetMaxEntrySize(k, 1, true);
    uint64_t prevtableentries = 1ULL << k;
    auto f1_sort_manager = std::make_unique<WriteOnlySortManager>(
        storage_context.WithSuffix(".p1.t1"),
        num_buckets,
        t1_entry_size_bytes,
        0,
        prevtableentries);

    // Multiple threads not implemented.
    F1SingleThread(0, k, id.data(), f1_sort_manager.get());
    f1_start_time.PrintElapsed("F1 complete, time:");

    return std::make_pair(
      Phase1TableResults{
        .num_entries = prevtableentries,
        .sort_manager = std::make_unique<ReadOnlySortManager>(
          std::move(f1_sort_manager), stripe_size),
        .table_sizes = {0},
        .read_files = {{}},
      },
      []{}
    );
}

}  // namespace

// This is Phase 1, or forward propagation. During this phase, all of the 7 tables,
// and f functions, are evaluated. The result is an intermediate plot file, that is
// several times larger than what the final file will be, but that has all of the
// proofs of space in it. First, F1 is computed, which is special since it uses
// ChaCha8, and each encryption provides multiple output values. Then, the rest of the
// f functions are computed, and a sort on disk happens for each table.
Phase1Result RunPhase1(
    StorageContext storage_context,
    uint8_t const k,
    const std::vector<uint8_t>& id,
    uint32_t const num_buckets,
    uint32_t const stripe_size,
    uint8_t const num_threads,
    uint8_t const flags)
{
    auto prev_table_results = checkpoint::Wrap<Phase1TableResults>(
        "phase1_table_0", id, k,
        RunPhase1Table0,
        storage_context,
        k,
        id,
        num_buckets,
        stripe_size,
        num_threads);

    // For tables 1 through 6, sort the table, calculate matches, and write
    // the next table. This is the left table index.
    for (uint8_t table_index = 1; table_index < 7; table_index++) {
      prev_table_results = checkpoint::Wrap<Phase1TableResults>(
        fmt::format("phase1_table_{}", table_index), id, k,
        RunPhase1OnTable,
        table_index,
        std::move(prev_table_results),
        storage_context,
        k,
        id.data(),
        num_buckets,
        stripe_size,
        num_threads,
        flags
      );
    }

    return Phase1Result{
      .read_files = std::move(prev_table_results.read_files),
      .table_sizes = std::move(prev_table_results.table_sizes),
    };
}

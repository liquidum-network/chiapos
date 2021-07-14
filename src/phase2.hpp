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

#ifndef SRC_CPP_PHASE2_HPP_
#define SRC_CPP_PHASE2_HPP_

#include <vector>

#include "disk.hpp"
#include "filtered_disk.hpp"
#include "phase1.hpp"
#include "readonly_sort_manager.hpp"
#include "storage.hpp"

struct Phase2Results {
    ReadDisk& disk_for_table(int const table_index)
    {
        if (table_index == 1) {
            if (!table1_initialized) {
              // TODO: This prevents us from resetting twice. Need to do this better.
              table1 = FilteredDisk(std::move(table1_info));
              table1_initialized = true;
            }
            return table1;
        } else if (table_index == 7) {
            return table7;
        } else {
            return *output_files[table_index - 2];
        }
    }

    FilteredDiskInfo table1_info;
    StreamingReadFile table7;
    std::array<std::unique_ptr<ReadOnlySortManager>, 5> output_files;
    std::array<size_t, 8> table_sizes;
    FilteredDisk table1;
    bool table1_initialized = false;
};

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
    uint8_t const flags);

#endif  // SRC_CPP_PHASE2_HPP

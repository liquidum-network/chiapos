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

#ifndef SRC_CPP_PHASE1_HPP_
#define SRC_CPP_PHASE1_HPP_

#include <array>

#include "disk.hpp"
#include "storage.hpp"

struct Phase1Result {
  std::array<FileInfo, 8> read_files;

  // These are used for sorting on disk. The sort on disk code needs to know how
  // many elements are in each bucket.
  std::array<size_t, 8> table_sizes;
};

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
    uint8_t const flags);

#endif  // SRC_CPP_PHASE1_HPP

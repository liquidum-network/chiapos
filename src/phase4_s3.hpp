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

#ifndef SRC_CPP_PHASE4_S3_HPP_
#define SRC_CPP_PHASE4_S3_HPP_

#include "final_file.hpp"
#include "phase3_s3.hpp"

struct Phase4Results {
    std::vector<size_t> final_table_begin_pointers;
    FinalFile final_file;
};

Phase4Results RunPhase4S3(
    const std::vector<uint8_t>& id,
    uint8_t k,
    uint8_t pos_size,
    Phase3Results res,
    const uint8_t flags,
    const int max_phase4_progress_updates);

#endif  // SRC_CPP_PHASE4_HPP

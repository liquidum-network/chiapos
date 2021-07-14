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

#ifndef SRC_CPP_DISK_HPP_
#define SRC_CPP_DISK_HPP_

#include "nlohmann/json.hpp"
#include "storage.hpp"

using json = nlohmann::json;

struct ReadDisk {
    virtual const uint8_t *BorrowAlignedAt(size_t begin, size_t length) = 0;
    virtual const uint8_t *BorrowOneAligned() = 0;
    virtual void Close() = 0;
    virtual void CloseAndDelete() = 0;
    virtual void FreeMemory() = 0;
    virtual ~ReadDisk() = default;
};

struct FileInfo {
    StorageContext storage_context;
    size_t size = 0;
    size_t alignment = 0;
};

inline void to_json(json& j, const FileInfo& p)
{
    if (p.size == 0) {
      j = nullptr;
      return;
    }
    j = json{
        {"storage_context", p.storage_context},
        {"size", p.size},
    };
    if (p.alignment != 0) {
      j["alignment"] = p.alignment;
    }
}

inline void from_json(const json& j, FileInfo& p)
{
    if (j == nullptr) {
      p = {};
      return;
    }
    j.at("storage_context").get_to(p.storage_context);
    j.at("size").get_to(p.size);
    p.alignment = j.value("alignment", 0);
}

#endif  // SRC_CPP_DISK_HPP_

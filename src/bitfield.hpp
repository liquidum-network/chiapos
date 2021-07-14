// Copyright 2020 Chia Network Inc

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>

#include "util.hpp"

struct Bitfield
{
    Bitfield() : size_(0) {}

    explicit Bitfield(int64_t size_bits)
        : buffer_(new uint64_t[(size_bits + 63) / 64]())
        , size_((size_bits + 63) / 64)
    {
    }

    void set(int64_t const bit)
    {
        if (size_ == 0) {
          return;
        }
        assert(bit / 64 < size_);
        buffer_[bit / 64] |= uint64_t(1) << (bit % 64);
    }

    bool get(int64_t const bit) const
    {
        if (size_ == 0) {
          return false;
        }
        assert(bit / 64 < size_);
        return (buffer_[bit / 64] & (uint64_t(1) << (bit % 64))) != 0;
    }

    void clear()
    {
        std::memset(buffer_.get(), 0, size_ * 8);
    }

    int64_t size() const { return size_ * 64; }

    const uint8_t * data() const { return reinterpret_cast<const uint8_t*>(buffer_.get()); }
    uint8_t * data() { return reinterpret_cast<uint8_t*>(buffer_.get()); }
    size_t size_bytes() const { return size_ * 8; }

    int64_t count(int64_t const start_bit, int64_t const end_bit) const
    {
        if (size_ == 0) {
          return 0;
        }
        assert((start_bit % 64) == 0);
        assert(start_bit <= end_bit);

        uint64_t const* start = buffer_.get() + start_bit / 64;
        uint64_t const* end = buffer_.get() + end_bit / 64;
        int64_t ret = 0;
        while (start != end) {
            ret += Util::PopCount(*start);
            ++start;
        }
        int const tail = end_bit % 64;
        if (tail > 0) {
            uint64_t const mask = (uint64_t(1) << tail) - 1;
            ret += Util::PopCount(*end & mask);
        }
        return ret;
    }

    void free_memory()
    {
        buffer_.reset();
        // Don't reset size so that we remember to delete.
    }
private:
    std::unique_ptr<uint64_t[]> buffer_;

    // number of 64-bit words
    int64_t size_;
};

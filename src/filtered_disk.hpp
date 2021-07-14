#ifndef SRC_CPP_FILTERED_DISK_HPP_
#define SRC_CPP_FILTERED_DISK_HPP_

#include <string>

#include "bitfield_s3_backed.hpp"
#include "logging.hpp"
#include "s3_sync_read_file.hpp"
#include "s3_sync_write_file.hpp"
#include "types.hpp"
#include "util.hpp"

struct FilteredDiskInfo {
    FileInfo file_info;
    BitfieldS3Backed bitfield_info;
    int entry_size;
};

using json = nlohmann::json;
inline void to_json(json &j, const FilteredDiskInfo &p)
{
    json bitfield_json;
    to_json(bitfield_json, p.bitfield_info);

    j = json{
        {"file_info", p.file_info},
        {"bitfield_info", std::move(bitfield_json)},
        {"entry_size", p.entry_size},
    };
}

inline void from_json(const json &j, FilteredDiskInfo &p)
{
    j.at("file_info").get_to(p.file_info);
    j.at("bitfield_info").get_to(p.bitfield_info);
    j.at("entry_size").get_to(p.entry_size);
}

struct FilteredDisk : ReadDisk {
    FilteredDisk() {}

    FilteredDisk(FilteredDiskInfo filtered_disk_info)
        : underlying_(std::move(filtered_disk_info.file_info)),
          bitfield_info_(std::move(filtered_disk_info.bitfield_info)),
          entry_size_(filtered_disk_info.entry_size)
    {
        bitfield_info_.EnsureLoaded();

        assert(entry_size_ > 0);
        DCHECK_GT(filter().size(), 0);
        DCHECK(filter().data() != nullptr);
        while (!filter().get(last_idx_)) {
            last_physical_ += entry_size_;
            ++last_idx_;
        }
        assert(filter().get(last_idx_));
        assert(last_physical_ == last_idx_ * entry_size_);
    }

    uint8_t const *BorrowAlignedAt(uint64_t begin, uint64_t length) override final
    {
        // we only support a single read-pass with no going backwards
        assert(begin >= last_logical_);
        assert((begin % entry_size_) == 0);
        assert(filter().get(last_idx_));
        assert(last_physical_ == last_idx_ * entry_size_);

        if (begin > last_logical_) {
            // last_idx_ et.al. always points to an entry we have (i.e. the bit
            // is set). So when we advance from there, we always take at least
            // one step on all counters.
            last_logical_ += entry_size_;
            last_physical_ += entry_size_;
            ++last_idx_;

            while (begin > last_logical_) {
                if (filter().get(last_idx_)) {
                    last_logical_ += entry_size_;
                }
                last_physical_ += entry_size_;
                ++last_idx_;
            }

            while (!filter().get(last_idx_)) {
                last_physical_ += entry_size_;
                ++last_idx_;
            }
        }

        assert(filter().get(last_idx_));
        assert(last_physical_ == last_idx_ * entry_size_);
        assert(begin == last_logical_);
        return underlying_.BorrowAlignedAt(last_physical_, length);
    }

    uint8_t const *BorrowOneAligned() override final { CHECK(false); }

    void Close() override final
    {
        underlying_.Close();
        filter().free_memory();
    }

    void CloseAndDelete() override final
    {
        underlying_.CloseAndDelete();
        bitfield_info_.Delete();
        filter().free_memory();
    }

    std::string GetFileName() const { return underlying_.GetFileName(); }
    size_t GetFileSize() const { return underlying_.GetFileSize(); }
    void FreeMemory() override final
    {
        filter().free_memory();
        underlying_.FreeMemory();
    }

    Bitfield &filter() { return bitfield_info_.bitfield_no_check_loaded(); }

private:
    // only entries whose bit is set should be read
    StreamingReadFile underlying_;
    BitfieldS3Backed bitfield_info_;
    int entry_size_;

    // the "physical" disk offset of the last read
    uint64_t last_physical_ = 0;
    // the "logical" disk offset of the last read. i.e. the offset as if the
    // file would have been compacted based on filter()
    uint64_t last_logical_ = 0;

    // the index of the last read. This is also the index into the bitfield. It
    // could be computed as last_physical_ / entry_size_, but we want to avoid
    // the division.
    uint64_t last_idx_ = 0;
};

#endif  // SRC_CPP_FILTERED_DISK_HPP_

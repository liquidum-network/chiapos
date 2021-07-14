#ifndef SRC_CPP_READONLY_SORT_MANAGER_HPP_
#define SRC_CPP_READONLY_SORT_MANAGER_HPP_

#include <algorithm>
#include <cmath>
#include <future>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "aws_async.hpp"
#include "bits.hpp"
#include "bucket_sort.hpp"
#include "calculate_bucket.hpp"
#include "chia_filesystem.hpp"
#include "disk.hpp"
#include "exceptions.hpp"
#include "s3_plotter.hpp"
#include "storage.hpp"
#include "writeonly_sort_manager.hpp"

enum class SortStrategy : uint8_t {
    uniform,
    quicksort,

    // the quicksort_last strategy is important because uniform sort performs
    // really poorly on data that isn't actually uniformly distributed. The last
    // buckets are often not uniformly distributed.
    quicksort_last,
};

class ReadOnlySortManager : public ReadDisk {
public:
    ReadOnlySortManager(
        std::unique_ptr<WriteOnlySortManager> write_manager,
        uint32_t stripe_size,
        SortStrategy sort_strategy = SortStrategy::uniform,
        double bucket_uniformity_factor = 1.5)
        : entry_size_(write_manager->entry_size()),
          log_num_buckets_(write_manager->log_num_buckets()),
          begin_bits_(write_manager->begin_bits()),
          stripe_size_(stripe_size),
          sort_strategy_(sort_strategy),
          bucket_uniformity_factor_(bucket_uniformity_factor)
    {
        write_manager->StartClose();

        // Flush the first one so we can prefetch bucket 0 for sorting.
        // NOTE: Have to make sure not to resize the vector,
        // as we assume the files don't move throughout this class.
        files_.reserve(write_manager->num_buckets());
        files_.emplace_back(write_manager->GetBucketFileInfo(0, entry_size_));

        auto maybe_scheduled_sort = num_readahead() > 0
                                        ? MaybeScheduleSort(0, false)
                                        : std::optional<std::future<ReadaheadResult>>{};
        if (maybe_scheduled_sort) {
            pending_sorts_[0] = std::move(*maybe_scheduled_sort);
        }

        for (size_t i = 1; i < write_manager->num_buckets(); ++i) {
            files_.emplace_back(write_manager->GetBucketFileInfo(i, entry_size_));
        }
    }

    ReadOnlySortManager(
        std::vector<FileInfo> file_infos,
        size_t entry_size,
        size_t begin_bits,
        uint32_t stripe_size,
        SortStrategy sort_strategy,
        double bucket_uniformity_factor)
        : entry_size_(entry_size),
          log_num_buckets_(std::log2(file_infos.size())),
          begin_bits_(begin_bits),
          stripe_size_(stripe_size),
          sort_strategy_(sort_strategy),
          bucket_uniformity_factor_(bucket_uniformity_factor)
    {
        // Do not do readahead here, the files are already written.
        // NOTE: Have to make sure not to resize the vector,
        // as we assume the files don't move throughout this class.
        files_.reserve(file_infos.size());
        for (size_t i = 0; i < file_infos.size(); ++i) {
            files_.emplace_back(
                S3SyncReadFile(file_infos[i].storage_context, file_infos[i].size, entry_size_));
        }
    }

    ReadOnlySortManager(ReadOnlySortManager&&) = delete;

    uint8_t const* BorrowAlignedAt(uint64_t begin, uint64_t length) override final
    {
        assert(length <= entry_size_);
        return ReadEntry(begin);
    }

    uint8_t const* BorrowOneAligned() override final { CHECK(false); }

    void Close() override final
    {
        FreeMemory();
        for (auto& file : files_) {
            file.Close();
        }
    }

    void CloseAndDelete() override final
    {
        FreeMemory();
        for (auto& file : files_) {
            file.CloseAndDelete();
        }
    }

    void FreeMemory() override final
    {
        prev_bucket_buf_.reset();
        read_memory_.reset();
        final_position_end = 0;
    }

    uint8_t* ReadEntry(uint64_t position)
    {
        if (position < this->final_position_start) {
            DCHECK_GE(position, prev_bucket_position_start, "Invalid prev bucket start");
            // this is allocated lazily, make sure it's here
            DCHECK(prev_bucket_buf_.get());
            DCHECK_LE(position - prev_bucket_position_start, prev_bucket_buf_size());
            return prev_bucket_buf_.get() + (position - prev_bucket_position_start);
        }

        while (position >= this->final_position_end) {
            SortBucket();
        }

        DCHECK_GT(final_position_end, position, "Position too large");
        DCHECK_LE(final_position_start, position, "Position too small");
        DCHECK(read_memory_);
        DCHECK_LE(position - final_position_start, read_memory_.size());
        return read_memory_.get() + (position - final_position_start);
    }

    bool CloseToNewBucket(uint64_t position) const
    {
        if (!(position <= this->final_position_end)) {
            return next_bucket_to_sort_ < num_buckets();
        };
        return (
            position + prev_bucket_buf_size() / 2 >= this->final_position_end &&
            next_bucket_to_sort_ < num_buckets());
    }

    void TriggerNewBucket(uint64_t position)
    {
        DCHECK_LE(position, final_position_end, "Triggering bucket too late");
        DCHECK_GE(position, final_position_start, "Triggering bucket too early");

        if (read_memory_) {
            // save some of the current bucket, to allow some reverse-tracking
            // in the reading pattern,
            // position is the first position that we need in the new array
            uint64_t const cache_size = (final_position_end - position);
            prev_bucket_buf_.reset(new uint8_t[prev_bucket_buf_size()]());
            memcpy(
                prev_bucket_buf_.get(),
                read_memory_.get() + position - final_position_start,
                cache_size);
        }

        SortBucket();
        prev_bucket_position_start = position;
    }

    const size_t num_buckets() const { return files_.size(); }

private:
    using ReadaheadResult = std::tuple<std::string, ManagedMemory, size_t>;

    friend void to_json(json& j, const std::unique_ptr<ReadOnlySortManager>& p);
    friend void from_json(const json& j, std::unique_ptr<ReadOnlySortManager>& p);

    uint32_t num_readahead() const
    {
        if (num_buckets() == 256) {
            return 2;
        } else {
            return 1;
        }
    }

    size_t prev_bucket_buf_size() const
    {
        return 2 * (stripe_size_ + 10 * (kBC / pow(2, kExtraBits))) * entry_size_;
    }

    double bucket_uniformity_factor() const { return bucket_uniformity_factor_; }

    std::vector<FileInfo> GetBucketFileInfos() const
    {
        std::vector<FileInfo> result;
        for (const auto& file : files_) {
            result.emplace_back(
                FileInfo{.storage_context = file.GetStorageContext(), .size = file.GetFileSize()});
        }
        return result;
    }

    size_t GetBucketEntries(size_t bucket_index) const
    {
        auto& file = files_[bucket_index];
        auto result = file.GetFileSize() / entry_size_;
        DCHECK_EQ(result * entry_size_, file.GetFileSize());
        return result;
    }

    void SortBucket();

    ReadaheadResult GetAndScheduleSorts(size_t bucket_index);

    std::optional<std::future<ReadaheadResult>> MaybeScheduleSort(
        size_t bucket_index,
        bool run_synchronously);

    ReadaheadResult GetBucketSortedBucket(size_t bucket_index, ManagedMemory memory);
    ReadaheadResult GetUniformSortedBucket(size_t bucket_index, ManagedMemory memory);
    ReadaheadResult GetInPlaceSortedBucket(size_t bucket_index, ManagedMemory memory);

    static constexpr uint32_t kNumReadahead = 1;
    std::vector<StreamingReadFile> files_;

    const uint16_t entry_size_;
    const uint32_t log_num_buckets_;
    const uint32_t begin_bits_;
    const uint32_t stripe_size_;
    const SortStrategy sort_strategy_;
    const double bucket_uniformity_factor_;

    std::map<size_t, std::future<ReadaheadResult>> pending_sorts_;

    // The buffer we use to sort buckets in-memory
    ManagedMemory read_memory_;

    std::unique_ptr<uint8_t[]> prev_bucket_buf_;
    uint64_t prev_bucket_position_start = 0;

    uint64_t final_position_start = 0;
    uint64_t final_position_end = 0;
    uint64_t next_bucket_to_sort_ = 0;
};

void to_json(json& j, const std::unique_ptr<ReadOnlySortManager>& p);
void from_json(const json& j, std::unique_ptr<ReadOnlySortManager>& p);

#endif  // SRC_CPP_READONLY_SORT_MANAGER_HPP_

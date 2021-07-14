#ifndef SRC_CPP_WRITEONLY_SORT_MANAGER_HPP_
#define SRC_CPP_WRITEONLY_SORT_MANAGER_HPP_

#include <algorithm>
#include <string>
#include <vector>

#include "bits.hpp"
#include "calculate_bucket.hpp"
#include "exceptions.hpp"
#include "s3_sync_write_file.hpp"
#include "storage.hpp"

class WriteOnlySortManager {
public:
    WriteOnlySortManager(
        StorageContext storage_context,
        uint32_t const num_buckets,
        uint16_t const entry_size,
        uint32_t begin_bits,
        size_t num_entries_upper_bound,
        double size_factor = 1.0)
        : entry_size_(entry_size),
          log_num_buckets_(std::log2(num_buckets)),
          begin_bits_(begin_bits),
          // 7 bytes head-room for SliceInt64FromBytes()
          entry_buf_(new uint8_t[entry_size + 7]),
          bucket_byte_size_upper_bound_(static_cast<size_t>(
              entry_size * size_factor * num_entries_upper_bound *
              SizeOverheadForNumBuckets(num_buckets) / num_buckets))
    {
        for (size_t bucket_i = 0; bucket_i < num_buckets; bucket_i++) {
            const auto bucket_number_padded = fmt::format("{:03}", bucket_i);

            files_.emplace_back(
                storage_context.WithSuffix(".sort_bucket_" + bucket_number_padded + ".tmp"),
                entry_size,
                bucket_byte_size_upper_bound_);
        }
    }

    WriteOnlySortManager(WriteOnlySortManager&&) = delete;
    WriteOnlySortManager(const WriteOnlySortManager&) = delete;

    ~WriteOnlySortManager()
    {
        StartClose();
        for (auto& file : files_) {
            file.AwaitClose();
        }
    }

    void Flush()
    {
        for (auto& file : files_) {
            if (file.GetFileSize() == 0) {
                break;
            }
            file.Flush();
        }
    }

    void StartClose()
    {
        if (close_started_) {
            return;
        }
        LogBucketStatistics();
        for (auto& file : files_) {
            file.StartClose();
        }
        close_started_ = true;
    }

    void AddToCache(const uint128_t& entry)
    {
        Util::IntTo16Bytes(entry_buf_.get(), entry);
        return AddToCache(entry_buf_.get());
    }

    template <typename T>
    void AddToCache(const BitsGeneric<T>& entry)
    {
        entry.ToBytes(entry_buf_.get());
        return AddToCache(entry_buf_.get());
    }

    void AddToCache(const uint8_t* entry)
    {
        uint64_t const bucket_index =
            Util::ExtractNum(entry, entry_size_, begin_bits_, log_num_buckets_);
        GetFile(bucket_index).WriteAppendAligned(entry);
    }

    template <typename T>
    void AddToCacheFlushLast(const BitsGeneric<T>& entry)
    {
        entry.ToBytes(entry_buf_.get());
        return AddToCacheFlushLast(entry_buf_.get());
    }

    void AddToCacheFlushLast(const uint8_t* entry)
    {
        uint64_t const bucket_index =
            Util::ExtractNum(entry, entry_size_, begin_bits_, log_num_buckets_);
        GetFile(bucket_index).WriteAppendAligned(entry);

        if (bucket_index + 1 == num_buckets() || files_[bucket_index + 1].GetFileSize() == 0) {
            GetFile(bucket_index).Flush();
        }
    }

    FileInfo GetBucketFileInfo(size_t idx, size_t read_alignment)
    {
        GetFile(idx).AwaitClose();
        return FileInfo{
            .storage_context = GetFile(idx).GetStorageContext(),
            .size = GetFile(idx).GetFileSize(),
            .alignment = read_alignment};
    }
    const size_t num_buckets() const { return files_.size(); }

    const uint32_t entry_size() const { return entry_size_; }
    const uint32_t log_num_buckets() const { return log_num_buckets_; }
    const uint32_t begin_bits() const { return begin_bits_; }

private:
    StreamingWriteFile& GetFile(size_t idx) { return files_[idx]; }

    void LogBucketStatistics() const
    {
        auto sizes = std::vector<size_t>{};
        for (const auto& file : files_) {
            if (file.GetFileSize() > 0) {
                sizes.push_back(file.GetFileSize());
            }
        }
        if (sizes.size() == 0) {
            return;
        }

        const auto min_size = *std::min_element(std::begin(sizes), std::end(sizes));
        const auto max_size = *std::max_element(std::begin(sizes), std::end(sizes));
        const auto total_size = std::reduce(std::begin(sizes), std::end(sizes));
        const auto avg_size = static_cast<double>(total_size) / sizes.size();

        SPDLOG_INFO(
            "bucket sizes for {}: min={} max={} avg={} total={} max/avg={:.3f} max/bound={:.3f}",
            files_[0].GetStorageContext().local_filename,
            min_size,
            max_size,
            static_cast<size_t>(avg_size),
            total_size,
            static_cast<double>(max_size) / avg_size,
            static_cast<double>(max_size) / bucket_byte_size_upper_bound_);
        SPDLOG_TRACE(
            "all bucket sizes {}: {}", files_[0].GetStorageContext().local_filename, sizes);
    }

    static double SizeOverheadForNumBuckets(size_t num_buckets)
    {
        if (num_buckets > 128) {
            return 1.06;
        } else if (num_buckets == 128) {
            return 1.04;
        } else {
            return 1.02;
        }
    }

    // Size of each entry
    const uint16_t entry_size_;
    // Log of the number of buckets; num bits to use to determine bucket
    const uint32_t log_num_buckets_;
    const uint32_t begin_bits_;
    std::unique_ptr<uint8_t[]> entry_buf_;
    std::vector<StreamingWriteFile> files_;
    const size_t bucket_byte_size_upper_bound_;
    bool close_started_ = false;
};

#endif  // SRC_CPP_WRITEONLY_SORT_MANAGER_HPP_

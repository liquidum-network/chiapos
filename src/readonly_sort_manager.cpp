#include "readonly_sort_manager.hpp"

#include <algorithm>
#include <cmath>
#include <future>
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
#include "experiments.hpp"
#include "quicksort.hpp"
#include "quicksort_entries.hpp"
#include "s3_plotter.hpp"
#include "storage.hpp"
#include "time_helpers.hpp"
#include "uniformsort.hpp"
#include "writeonly_sort_manager.hpp"

namespace {
// The number of memory entries required to do the custom SortInMemory algorithm, given the
// total number of entries to be sorted.
uint64_t RoundSize(uint64_t size)
{
    size *= 2;
    uint64_t result = 1;
    while (result < size) result *= 2;
    return result + 50;
}
}  // namespace

using ReadaheadResult = std::tuple<std::string, ManagedMemory, size_t>;

void ReadOnlySortManager::SortBucket()
{
    CHECK_LT(next_bucket_to_sort_, num_buckets(), "Trying to sort bucket which does not exist.");

    // Deallocate existing memory first to relieve pressure.
    // TODO: Maybe we should try to reuse this buffer if possible.
    read_memory_.reset();

    TimeLogger sort_time_logger;
    auto [error_message, memory, read_size] = GetAndScheduleSorts(next_bucket_to_sort_);

    auto duration = sort_time_logger.duration();
    if (duration > 8.0) {
        SPDLOG_INFO(
            "Sorting {} took {:.3f} seconds", files_[next_bucket_to_sort_].GetFileName(), duration);
    }

    CHECK_EQ(error_message, "");
    read_memory_ = std::move(memory);

    this->final_position_start = this->final_position_end;
    this->final_position_end += read_size;
    next_bucket_to_sort_ += 1;
}

ReadaheadResult ReadOnlySortManager::GetAndScheduleSorts(size_t bucket_index)
{
    std::future<ReadaheadResult> this_bucket_future;
    const auto iter = pending_sorts_.find(bucket_index);
    if (iter != pending_sorts_.end()) {
        this_bucket_future = std::move(iter->second);
        pending_sorts_.erase(iter);
    }

    // Make sure the next one is also running.
    for (auto next_bucket_index = bucket_index + 1;
         next_bucket_index < std::min(bucket_index + 1 + num_readahead(), num_buckets());
         ++next_bucket_index) {
        const auto next_iter = pending_sorts_.find(next_bucket_index);
        if (next_iter == pending_sorts_.end()) {
            auto maybe_scheduled_sort = MaybeScheduleSort(next_bucket_index, false);
            if (maybe_scheduled_sort) {
                pending_sorts_[next_bucket_index] = std::move(*maybe_scheduled_sort);
            } else {
                SPDLOG_DEBUG("Could not allocate for bucket readahead {}", next_bucket_index);
                break;
            }
        }
    }

    if (this_bucket_future.valid()) {
        return this_bucket_future.get();
    } else {
        SPDLOG_INFO(
            "Sort bucket file {} not ready, sorting synchronously",
            files_[bucket_index].GetFileName());
        auto result = (*MaybeScheduleSort(bucket_index, true)).get();
        return result;
    }
}

ReadaheadResult ReadOnlySortManager::GetBucketSortedBucket(
    size_t bucket_index,
    ManagedMemory memory)
{
    auto& file = files_[bucket_index];
    SPDLOG_TRACE("In place sorting bucket {}", file.GetFileName());

    const auto bucket_entries = GetBucketEntries(bucket_index);
    const auto read_size = bucket_entries * entry_size_;
    DCHECK_EQ(read_size, file.GetFileSize());
    DCHECK_GE(memory.size(), read_size);

    BucketSort::SortToMemory<8>(
        &file,
        memory.get(),
        memory.size(),
        entry_size_,
        bucket_entries,
        begin_bits_ + log_num_buckets_);

    SPDLOG_TRACE("Done bucket {}", file.GetFileName());
    return {"", std::move(memory), read_size};
}

ReadaheadResult ReadOnlySortManager::GetUniformSortedBucket(
    size_t bucket_index,
    ManagedMemory memory)
{
    auto& file = files_[bucket_index];
    SPDLOG_TRACE("Uniform sorting bucket {}", file.GetFileName());
    const auto bucket_entries = GetBucketEntries(bucket_index);
    const size_t read_size = bucket_entries * entry_size_;
    DCHECK_EQ(read_size, file.GetFileSize());

    DCHECK_GE(memory.size(), RoundSize(bucket_entries) * entry_size_);

    UniformSort::SortToMemory(
        file,
        memory.get(),
        memory.size(),
        entry_size_,
        bucket_entries,
        begin_bits_ + log_num_buckets_);

    SPDLOG_TRACE("Done sorting bucket {}", file.GetFileName());
    return {"", std::move(memory), read_size};
}

ReadaheadResult ReadOnlySortManager::GetInPlaceSortedBucket(
    size_t bucket_index,
    ManagedMemory memory)
{
    auto& file = files_[bucket_index];
    SPDLOG_TRACE("In place sorting bucket {}", file.GetFileName());

    const auto bucket_entries = GetBucketEntries(bucket_index);
    const auto read_size = bucket_entries * entry_size_;
    DCHECK_EQ(read_size, file.GetFileSize());
    DCHECK_GE(memory.size(), read_size);

    file.ReadEntireFile(memory.get(), read_size);
    file.Close();

    SortEntriesInPlace(memory.get(), read_size, entry_size_, begin_bits_ + log_num_buckets_);

    SPDLOG_TRACE("Done bucket {}", file.GetFileName());
    return {"", std::move(memory), read_size};
}

std::optional<std::future<ReadaheadResult>> ReadOnlySortManager::MaybeScheduleSort(
    size_t bucket_index,
    bool run_synchronously)
{
    if (files_[bucket_index].GetFileSize() == 0) {
        return {};
    }
    // Try to use uniform sort but fallback if we can't get enough
    // memory.
    const bool last_bucket =
        (bucket_index == num_buckets() - 1) || files_[bucket_index + 1].GetFileSize() == 0;

    const bool force_quicksort = (sort_strategy_ == SortStrategy::quicksort) ||
                                 (sort_strategy_ == SortStrategy::quicksort_last && last_bucket);

    const auto bucket_entries = GetBucketEntries(bucket_index);
    if (!force_quicksort) {
#if 0
        // Test more and fix edge case of full buckets before shipping.
        if (!GetExperimentBool("DISABLE_BUCKET_SORT")) {
            uint64_t const memory_required_for_bucket_sort =
                bucket_uniformity_factor() * bucket_entries * entry_size_;
            auto bucket_sort_memory =
                ManagedMemory::TryAllocate(memory_required_for_bucket_sort, 0, 500);
            if (bucket_sort_memory) {
                if (run_synchronously) {
                    return create_synchronous_future(
                        GetBucketSortedBucket(bucket_index, std::move(bucket_sort_memory)));
                } else {
                    return s3_plotter::shared_thread_pool_->SubmitAndGetFuture(
                        [this, bucket_index, m = std::move(bucket_sort_memory)]() mutable {
                            return GetBucketSortedBucket(bucket_index, std::move(m));
                        });
                }
            }
        }
#endif

        uint64_t const memory_required_for_uniform_sort = RoundSize(bucket_entries) * entry_size_;
        auto memory = ManagedMemory::TryAllocate(memory_required_for_uniform_sort, 0, 500);
        if (memory) {
            if (run_synchronously) {
                return create_synchronous_future(
                    GetUniformSortedBucket(bucket_index, std::move(memory)));
            } else {
                return s3_plotter::shared_thread_pool_->SubmitAndGetFuture(
                    [this, bucket_index, m = std::move(memory)]() mutable {
                        return GetUniformSortedBucket(bucket_index, std::move(m));
                    });
            }
        }
        SPDLOG_WARN(
            "Not enough memory to uniform sort, doing in-place for {}",
            files_[bucket_index].GetStorageContext().local_filename);
    }

    auto memory = run_synchronously
                      ? ManagedMemory(bucket_entries * entry_size_)
                      : ManagedMemory::TryAllocate(bucket_entries * entry_size_, 0, 1000);
    if (memory) {
        if (run_synchronously) {
            return create_synchronous_future(
                GetInPlaceSortedBucket(bucket_index, std::move(memory)));
        } else {
            return s3_plotter::shared_thread_pool_->SubmitAndGetFuture(
                [this, bucket_index, m = std::move(memory)]() mutable {
                    return GetInPlaceSortedBucket(bucket_index, std::move(m));
                });
        }
    }

    return {};
}

void to_json(json& j, const std::unique_ptr<ReadOnlySortManager>& p)
{
    if (p == nullptr) {
        j = nullptr;
        return;
    }

    j = json{
        {"file_infos", p->GetBucketFileInfos()},
        {"entry_size", p->entry_size_},
        {"begin_bits", p->begin_bits_},
        {"stripe_size", p->stripe_size_},
        {"sort_strategy", p->sort_strategy_},
        {"bucket_uniformity_factor", p->bucket_uniformity_factor_},
    };
}

void from_json(const json& j, std::unique_ptr<ReadOnlySortManager>& p)
{
    if (j == nullptr) {
        p = nullptr;
        return;
    }
    p = std::make_unique<ReadOnlySortManager>(
        j.at("file_infos"),
        j.at("entry_size"),
        j.at("begin_bits"),
        j.at("stripe_size"),
        j.at("sort_strategy"),
        j.at("bucket_uniformity_factor"));
}

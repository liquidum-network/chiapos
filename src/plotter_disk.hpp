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

#ifndef SRC_CPP_PLOTTER_DISK_HPP_
#define SRC_CPP_PLOTTER_DISK_HPP_

#ifndef _WIN32
#include <semaphore.h>
#include <sys/resource.h>
#include <unistd.h>
#endif

#include <math.h>
#include <stdio.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "aws_async.hpp"
#include "aws_globals.hpp"
#include "calculate_bucket.hpp"
#include "chia_filesystem.hpp"
#include "encoding.hpp"
#include "exceptions.hpp"
#include "final_file.hpp"
#include "logging.hpp"
#include "managed_memory.hpp"
#include "manifest.hpp"
#include "phase1.hpp"
#include "phase2.hpp"
#include "phase3_s3.hpp"
#include "phase4_s3.hpp"
#include "phases.hpp"
#include "pos_constants.hpp"
#include "s3_sync_read_file.hpp"
#include "storage.hpp"
#include "util.hpp"

class DiskPlotter {
public:
    // This method creates a plot on disk with the filename. Many temporary files
    // (filename + ".table1.tmp", filename + ".p2.t3.sort_bucket_4.tmp", etc.) are created
    // and their total size will be larger than the final plot file. Temp files are deleted at the
    // end of the process.
    void CreatePlotDisk(
        std::string tmp_dirname,
        std::string filename,
        uint8_t k,
        const std::vector<uint8_t>& memo,
        const std::vector<uint8_t>& id,
        uint32_t buf_megabytes_input = 0,
        uint32_t num_buckets_input = 0,
        uint64_t stripe_size_input = 0,
        uint8_t num_threads_input = 0,
        uint8_t phases_flags = ENABLE_BITFIELD)
    {
        if (k < kMinPlotSize || k > kMaxPlotSize) {
            throw InvalidValueException("Plot size k= " + std::to_string(k) + " is invalid");
        }
        assert(id.size() == kIdLen);

        uint64_t stripe_size, buf_megabytes;
        uint32_t num_buckets;
        uint8_t num_threads;
        if (stripe_size_input != 0) {
            stripe_size = stripe_size_input;
        } else {
            stripe_size = 65536;
        }
        if (num_threads_input != 0) {
            num_threads = num_threads_input;
        } else {
            num_threads = 2;
        }
        if (buf_megabytes_input != 0) {
            buf_megabytes = buf_megabytes_input;
        } else {
            buf_megabytes = 4608;
        }

        if (buf_megabytes < 10) {
            throw InsufficientMemoryException("Please provide at least 10MiB of ram");
        }

        constexpr uint64_t megabyte = 1024 * 1024;
        size_t max_memory_bytes = buf_megabytes * megabyte;

        // Subtract some ram to account for dynamic allocation through the code
        size_t thread_memory =
            num_threads * (2 * (stripe_size + 5000)) * EntrySizes::GetMaxEntrySize(k, 4, true);
        size_t extra_memory =
            (5 * megabyte + (size_t)std::min(max_memory_bytes * 0.05, 50.0 * megabyte) +
             thread_memory);

        if (num_buckets_input != 0) {
            num_buckets = Util::RoundPow2(num_buckets_input);
            CHECK_GE(num_buckets, kMinBuckets);
            CHECK_LE(num_buckets, kMaxBuckets);
        } else {
            num_buckets = 64;
        }
        num_buckets = std::max(kMinBuckets, std::min(kMaxBuckets, num_buckets));

        double max_table_size = 0;
        for (size_t i = 1; i <= 7; i++) {
            double memory_i = 1.3 * ((uint64_t)1 << k) * EntrySizes::GetMaxEntrySize(k, i, true);
            if (memory_i > max_table_size)
                max_table_size = memory_i;
        }
        size_t sort_memory_size = ceil(max_table_size / (num_buckets * kMemSortProportion));

        if (max_table_size / num_buckets < stripe_size * 30) {
            throw InvalidValueException("Stripe size too large");
        }

#if defined(_WIN32) || defined(__x86_64__)
        if (phases_flags & ENABLE_BITFIELD && !Util::HavePopcnt()) {
            throw InvalidValueException("Bitfield plotting not supported by CPU");
        }
#endif /* defined(_WIN32) || defined(__x86_64__) */

        const auto plot_id_hex = Util::HexStr(id.data(), id.size());
        // Need to make sure AWS has been initialized before calling this.
        s3_plotter::InitS3Client(num_buckets + num_threads + 7);
        CHECK_NE(GetAwsRegion(), "");

        // Use all available memory for buffers.
        CHECK_LT(extra_memory + 2 * sort_memory_size + 100 * megabyte, max_memory_bytes);
        size_t managed_memory_size = max_memory_bytes - extra_memory;
        SPDLOG_INFO(
            "Using {} MB total. {} managed, {} for everything else",
            max_memory_bytes / megabyte,
            managed_memory_size / megabyte,
            extra_memory / megabyte);

        InitMemoryManager(managed_memory_size);

        SPDLOG_INFO("Starting plotting");
        SPDLOG_INFO("ID: {}", plot_id_hex);
        SPDLOG_INFO("Plot size is: {}", static_cast<int>(k));
        SPDLOG_INFO("Buffer size is: {} MiB", buf_megabytes);
        SPDLOG_INFO("Using {} buckets", num_buckets);
        SPDLOG_INFO("Using {} threads of stripe size {}", (int)num_threads, stripe_size);

        const auto base_storage_context = StorageContext::CreateTemp(k, plot_id_hex, filename);

        SPDLOG_INFO("Starting phase 1/4: Forward Propagation into tmp files...");

        Timer p1;
        Timer all_phases;
        auto phase1_result = RunPhase1(
            base_storage_context,
            k,
            id,
            num_buckets,
            stripe_size,
            num_threads,
            phases_flags);
        p1.PrintElapsed("Time for phase 1 =");

        CHECK(phases_flags & ENABLE_BITFIELD, "Not implemented");
        SPDLOG_INFO("Starting phase 2/4: Backpropagation into tmp files...");

        Timer p2;
        Phase2Results res2 = RunPhase2(
            base_storage_context,
            std::move(phase1_result),
            k,
            id,
            num_buckets,
            phases_flags);
        p2.PrintElapsed("Time for phase 2 =");

        SPDLOG_INFO("Starting phase 3/4: Compression from tmp files to s3 ...");
        Timer p3;
        Phase3Results res3 = RunPhase3S3(
            base_storage_context,
            k,
            std::move(res2),
            id,
            num_buckets,
            phases_flags);

        p3.PrintElapsed("Time for phase 3 =");

        SPDLOG_INFO("Starting phase 4/4: Writing checkpoint tables to s3 ...");
        Timer p4;
        const auto phase4_result = RunPhase4S3(id, k, k + 1, std::move(res3), phases_flags, 16);
        p4.PrintElapsed("Time for phase 4 =");

        // Now we open a new file, where the final contents of the plot will be stored.

        // TODO: Check that written file sizes match the returned sizes.
        phase4_result.final_file.CheckComplete();
        
        // TODO: Copy parts to final location with get-range.
        // https://docs.aws.amazon.com/AmazonS3/latest/API/API_UploadPartCopy.html

        SPDLOG_INFO("Done plotting.");
        SPDLOG_INFO("Writing manifest.");

        auto manifest_bytes = CreatePlotManifest(
            phase4_result.final_file, k, id, memo, phase4_result.final_table_begin_pointers);
        SPDLOG_INFO("Uploading manifest to S3 {}", manifest_bytes);
        const auto [error_message, manifest_storage] =
            UploadManifestToS3(StorageContext::CreateForManifest(k, plot_id_hex), manifest_bytes);

        if (!error_message.empty()) {
            throw std::runtime_error(error_message);
        }

        SPDLOG_INFO("Wrote manifest to {}", manifest_storage.GetUri());
        if (!tmp_dirname.empty()) {
            const auto tmp_dir_path = fs::path(tmp_dirname);
            const auto local_path = (tmp_dir_path / plot_id_hex).string() + ".json";
            std::ofstream(
                local_path, std::ios_base::out | std::ios_base::binary | std::ios_base::trunc)
                << manifest_bytes;
            SPDLOG_INFO("Also Wrote manifest to {}", local_path);
        }

        // The total number of bytes used for sort is saved to table_sizes[0]. All other
        // elements in table_sizes represent the total number of entries written by the end of
        // phase 1 (which should be the highest total working space time). Note that the max
        // sort on disk space does not happen at the exact same time as max table sizes, so this
        // estimate is conservative (high).
        uint64_t total_working_space = phase1_result.table_sizes[0];
        for (size_t i = 1; i <= 7; i++) {
            total_working_space +=
                phase1_result.table_sizes[i] * EntrySizes::GetMaxEntrySize(k, i, false);
        }
        SPDLOG_INFO(
            "Approximate working space used (without final file): {} GiB",
            static_cast<double>(total_working_space) / (1024 * 1024 * 1024));

        uint64_t finalsize = phase4_result.final_table_begin_pointers[11];
        SPDLOG_INFO(
            "Final File size: {} Gib", static_cast<double>(finalsize) / (1024 * 1024 * 1024));

        all_phases.PrintElapsed("Total time =");

        AwaitAllPendingAwaitables();
    }
};

#endif  // SRC_CPP_PLOTTER_DISK_HPP_

#define USE_MOCK_S3 1
#define BUCKET_SORT_INPUT_FILE_TYPE MockFile
#define UNIFORM_SORT_INPUT_FILE_TYPE MockFile
#define SRC_CPP_S3_SYNC_READ_FILE_HPP_

#include <cstddef>
#include <string>

#include "../tests/mock_file.hpp"
#include "bucket_sort.hpp"
#include "logging.hpp"
#include "quicksort_entries.hpp"
#include "serialization.hpp"
#include "test_logging.hpp"
#include "time_helpers.hpp"
#include "uniformsort.hpp"
#include "util.hpp"

#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "../lib/include/catch.hpp"

static int _ = SetupTestLogging();

namespace {

void GetRandomBytes(uint8_t* buf, size_t num_bytes)
{
    for (size_t i = 0; i < num_bytes; ++i) {
        buf[i] = rand();
    }
}

constexpr auto kSortSize = 10000000;
constexpr auto kEntrySize = 27;

}  // namespace

void CheckMemorySorted(uint8_t* buffer, size_t size, size_t entry_size, size_t begin_bits)
{
    for (size_t entry = 1; entry < size / entry_size; ++entry) {
        CHECK_LT(
            Util::MemCmpBits(
                &buffer[(entry - 1) * entry_size],
                &buffer[entry * entry_size],
                entry_size,
                begin_bits),
            0,
            "mismatch at entry {}, {} > {}",
            entry - 1,
            RawBytesToHex(&buffer[(entry - 1) * entry_size], entry_size),
            RawBytesToHex(&buffer[entry * entry_size], entry_size));
    }
}

TEST_CASE("TestSortRandomInPlace")
{
    const size_t entry_size = kEntrySize;
    const size_t size = kSortSize * entry_size;
    const size_t begin_bits = 0;
    auto input_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[size]);
    SPDLOG_INFO("Filling buffer with {} random bytes", size);
    GetRandomBytes(input_buffer.get(), size);

    SPDLOG_INFO("Sorting with in-place sort");
    TimeLogger time_logger;
    SortEntriesInPlace(input_buffer.get(), size, entry_size, begin_bits);
    SPDLOG_INFO("Sorted in {} seconds", time_logger.duration());

    CheckMemorySorted(input_buffer.get(), size, entry_size, begin_bits);
}

TEST_CASE("TestSortRandomBucket")
{
    const size_t entry_size = kEntrySize;
    const size_t size = kSortSize * entry_size;
    const size_t begin_bits = 0;
    const auto target_read_size = 128 * 1024;
    auto input_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[size]);
    SPDLOG_INFO("Filling buffer with {} random bytes", size);
    GetRandomBytes(input_buffer.get(), size);

    auto output_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[4 * size]);

    auto file_mock =
        MockFile(input_buffer.get(), size, (target_read_size / entry_size) * entry_size);

    SPDLOG_INFO("Sorting with bucket sort");
    TimeLogger time_logger;
    BucketSort::SortToMemory<8>(
        &file_mock, output_buffer.get(), 4 * size, entry_size, size / entry_size, begin_bits);
    SPDLOG_INFO("Sorted in {} seconds", time_logger.duration());

    CheckMemorySorted(output_buffer.get(), size, entry_size, begin_bits);
}

TEST_CASE("TestSortRandomUniform")
{
    const size_t entry_size = kEntrySize;
    const size_t size = kSortSize * entry_size;
    const size_t begin_bits = 0;
    const auto target_read_size = 128 * 1024;
    auto input_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[size]);
    SPDLOG_INFO("Filling buffer with {} random bytes", size);
    GetRandomBytes(input_buffer.get(), size);

    auto file_mock =
        MockFile(input_buffer.get(), size, (target_read_size / entry_size) * entry_size);

    auto output_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[4 * size]);

    SPDLOG_INFO("Sorting with static uniform sort");
    TimeLogger time_logger;
    UniformSort::SortToMemory(
        file_mock, output_buffer.get(), 4 * size, entry_size, size / entry_size, begin_bits);
    SPDLOG_INFO("Sorted in {} seconds", time_logger.duration());

    CheckMemorySorted(output_buffer.get(), size, entry_size, begin_bits);
}

TEST_CASE("TestSortRandomUniformDynamic")
{
    const size_t entry_size = kEntrySize;
    const size_t size = kSortSize * entry_size;
    const size_t begin_bits = 0;
    const auto target_read_size = 128 * 1024;
    auto input_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[size]);
    SPDLOG_INFO("Filling buffer with {} random bytes", size);
    GetRandomBytes(input_buffer.get(), size);

    auto file_mock =
        MockFile(input_buffer.get(), size, (target_read_size / entry_size) * entry_size);

    auto output_buffer = std::unique_ptr<uint8_t[]>(new uint8_t[4 * size]);

    SPDLOG_INFO("Sorting with dynamic uniform sort");
    TimeLogger time_logger;
    UniformSort::SortToMemoryDynamic(
        file_mock, output_buffer.get(), 4 * size, entry_size, size / entry_size, begin_bits);
    SPDLOG_INFO("Sorted in {} seconds", time_logger.duration());

    CheckMemorySorted(output_buffer.get(), size, entry_size, begin_bits);
}

// Put all the code in this file so that we can mock out file types.
#include "quicksort_entries.cpp"
#include "uniformsort.cpp"

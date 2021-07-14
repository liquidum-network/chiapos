#define USE_MOCK_S3 1

#include "push_streambuf.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <random>
#include <sstream>
#include <string>
#include <thread>

#include "locking_concurrent_streambuf.hpp"
#include "test_logging.hpp"
#include "time_helpers.hpp"

#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "../lib/include/catch.hpp"

static int _ = SetupTestLogging();

namespace {

void SleepExponential(uint64_t avg_millis)
{
    std::mt19937_64 eng{std::random_device{}()};
    std::exponential_distribution<float> dist{1.0f / static_cast<float>(avg_millis)};
    std::this_thread::sleep_for(std::chrono::milliseconds{static_cast<int>(dist(eng))});
}

constexpr size_t kSmallIterations = 10000000;
constexpr size_t kBigIterations = 10 * kSmallIterations;

}  // namespace

TEST_CASE("TestPushBufFastDrainPerformance")
{
    InitMemoryManager(100 * 1024 * 1024);

    // Tests push performance with a fast drain.
    auto shared_buffer = PushStreambuf(64 * 1024, "test");
    constexpr auto length = 7;
    constexpr auto num_iterations = kBigIterations;

    auto writer = [&]() {
        char buffer[32] = {0};

        for (size_t i = 0; i < num_iterations; ++i) {
            CHECK_EQ(shared_buffer.sputn(buffer, length), length);
        }
        shared_buffer.SetWriteDone();
    };

    size_t bytes_read = 0;
    auto reader = [&]() {
        while (true) {
            const auto [_, __, this_bytes_read] = shared_buffer.BorrowGetBytes(1);
            shared_buffer.FinishGetBytes(this_bytes_read);
            bytes_read += this_bytes_read;
            if (this_bytes_read == 0) {
                break;
            }
        }
    };

    TimeLogger timer;
    auto writer_thread = std::thread(writer);
    auto reader_thread = std::thread(reader);
    reader_thread.join();
    writer_thread.join();
    SPDLOG_INFO("push read in {} seconds", timer.duration());

    SPDLOG_TRACE(shared_buffer.StateString("test"));
    CHECK_EQ(bytes_read, length * num_iterations);
}

TEST_CASE("TestLockingBufferFastDrainPerformance")
{
    InitMemoryManager(100 * 1024 * 1024);

    // Tests push performance with a fast drain.
    auto shared_buffer = LockingConcurrentStreamBuf(64 * 1024, "test");
    constexpr auto length = 7;
    constexpr auto num_iterations = kBigIterations;

    auto writer = [&]() {
        char buffer[32] = {0};

        for (size_t i = 0; i < num_iterations; ++i) {
            CHECK_EQ(shared_buffer.sputn(buffer, length), length);
        }
        shared_buffer.SetWriteDone();
    };

    size_t bytes_read = 0;
    auto reader = [&]() {
        while (true) {
            const auto [_, __, this_bytes_read] = shared_buffer.BorrowGetBytes(1);
            shared_buffer.FinishGetBytes(this_bytes_read);
            bytes_read += this_bytes_read;
            if (this_bytes_read == 0) {
                break;
            }
        }
    };

    TimeLogger timer;
    auto writer_thread = std::thread(writer);
    auto reader_thread = std::thread(reader);
    reader_thread.join();
    writer_thread.join();
    SPDLOG_INFO("locking read in {} seconds", timer.duration());

    SPDLOG_TRACE(shared_buffer.StateString("test"));
    CHECK_EQ(bytes_read, length * num_iterations);
}

TEST_CASE("TestPushBufSlowDrainPerformance")
{
    InitMemoryManager(100 * 1024 * 1024);

    // Tests push performance with a fast drain.
    auto shared_buffer = PushStreambuf(64 * 1024, "test");
    constexpr auto length = 7;
    constexpr auto num_iterations = kSmallIterations;

    auto writer = [&]() {
        char buffer[32] = {0};

        for (size_t i = 0; i < num_iterations; ++i) {
            CHECK_EQ(shared_buffer.sputn(buffer, length), length);
        }
        shared_buffer.SetWriteDone();
    };

    size_t bytes_read = 0;
    auto reader = [&]() {
        while (true) {
            const auto [_, __, this_bytes_read] = shared_buffer.BorrowGetBytes(1);
            shared_buffer.FinishGetBytes(this_bytes_read);
            bytes_read += this_bytes_read;
            if (this_bytes_read == 0) {
                break;
            }
            SleepExponential(1);
        }
    };

    TimeLogger timer;
    auto writer_thread = std::thread(writer);
    auto reader_thread = std::thread(reader);
    reader_thread.join();
    writer_thread.join();
    SPDLOG_INFO("push slow drain in {} seconds", timer.duration());

    SPDLOG_TRACE(shared_buffer.StateString("test"));
    CHECK_EQ(bytes_read, length * num_iterations);
}

TEST_CASE("TestLockingBufferSlowDrainPerformance")
{
    InitMemoryManager(100 * 1024 * 1024);

    // Tests push performance with a fast drain.
    auto shared_buffer = LockingConcurrentStreamBuf(64 * 1024, "test");
    constexpr auto length = 7;
    constexpr auto num_iterations = kSmallIterations;

    auto writer = [&]() {
        char buffer[32] = {0};

        for (size_t i = 0; i < num_iterations; ++i) {
            CHECK_EQ(shared_buffer.sputn(buffer, length), length);
        }
        shared_buffer.SetWriteDone();
    };

    size_t bytes_read = 0;
    auto reader = [&]() {
        while (true) {
            const auto [_, __, this_bytes_read] = shared_buffer.BorrowGetBytes(1);
            shared_buffer.FinishGetBytes(this_bytes_read);
            bytes_read += this_bytes_read;
            if (this_bytes_read == 0) {
                break;
            }
            SleepExponential(1);
        }
    };

    TimeLogger timer;
    auto writer_thread = std::thread(writer);
    auto reader_thread = std::thread(reader);
    reader_thread.join();
    writer_thread.join();
    SPDLOG_INFO("locking slow drain in {} seconds", timer.duration());

    SPDLOG_TRACE(shared_buffer.StateString("test"));

    CHECK_EQ(bytes_read, length * num_iterations);
}

TEST_CASE("TestPushBufSgetnPerformance")
{
    InitMemoryManager(100 * 1024 * 1024);

    // Tests push performance with a fast drain.
    auto shared_buffer = PushStreambuf(64 * 1024, "test");
    constexpr auto length = 7;
    constexpr auto num_iterations = kBigIterations;

    auto writer = [&]() {
        size_t bytes_written = 0;
        while (bytes_written < num_iterations * length) {
            const auto [_, __, this_bytes_write] = shared_buffer.BorrowPutBytes(1);
            shared_buffer.FinishPutBytes(std::min(
                static_cast<size_t>(this_bytes_write), num_iterations * length - bytes_written));
            bytes_written += this_bytes_write;
        }
        shared_buffer.SetWriteDone();
    };

    auto reader = [&]() {
        // Not divisible by 7, but divides total read size.
        constexpr size_t read_size = 3200;
        char buffer[read_size] = {0};

        for (size_t start_byte = 0; start_byte < num_iterations * length; start_byte += read_size) {
            CHECK_EQ(
                shared_buffer.sgetn(buffer, read_size), read_size, "start_byte={}", start_byte);
        }
        shared_buffer.SetReadDone();
    };

    TimeLogger timer;
    auto writer_thread = std::thread(writer);
    auto reader_thread = std::thread(reader);
    reader_thread.join();
    writer_thread.join();
    SPDLOG_INFO("push xsgetn in {} seconds", timer.duration());

    SPDLOG_TRACE(shared_buffer.StateString("test"));
}

TEST_CASE("TestLockingBufferSgetnPerformance")
{
    InitMemoryManager(100 * 1024 * 1024);

    // Tests push performance with a fast drain.
    auto shared_buffer = LockingConcurrentStreamBuf(64 * 1024, "test");
    constexpr auto length = 7;
    constexpr auto num_iterations = kBigIterations;

    auto writer = [&]() {
        size_t bytes_written = 0;
        while (bytes_written < num_iterations * length) {
            const auto [_, __, this_bytes_write] = shared_buffer.BorrowPutBytes(1);
            shared_buffer.FinishPutBytes(
                std::min(this_bytes_write, num_iterations * length - bytes_written));
            bytes_written += this_bytes_write;
        }
        shared_buffer.SetWriteDone();
    };

    auto reader = [&]() {
        // Not divisible by 7, but divides total read size.
        constexpr size_t read_size = 3200;
        char buffer[read_size] = {0};

        for (size_t start_byte = 0; start_byte < num_iterations * length; start_byte += read_size) {
            CHECK_EQ(
                shared_buffer.sgetn(buffer, read_size), read_size, "start_byte={}", start_byte);
        }
        shared_buffer.SetReadDone();
    };

    TimeLogger timer;
    auto writer_thread = std::thread(writer);
    auto reader_thread = std::thread(reader);
    reader_thread.join();
    writer_thread.join();
    SPDLOG_INFO("locking xsgetn in {} seconds", timer.duration());

    SPDLOG_TRACE(shared_buffer.StateString("test"));
}

TEST_CASE("TestPushBufCopyBothSides")
{
    InitMemoryManager(100 * 1024 * 1024);

    // Tests push performance with a fast drain.
    auto shared_buffer = PushStreambuf(64 * 1024, "test");
    constexpr auto length = 7;
    constexpr size_t num_iterations = kBigIterations;

    auto writer = [&]() {
        char buffer[32] = {0};

        for (size_t i = 0; i < num_iterations; ++i) {
            CHECK_EQ(shared_buffer.sputn(buffer, length), length);
        }
        shared_buffer.SetWriteDone();
    };

    size_t bytes_read = 0;
    auto reader = [&]() {
        // Not divisible by 7, but divides total read size.
        constexpr size_t read_size = 3200;
        char buffer[read_size] = {0};

        for (size_t start_byte = 0; start_byte < num_iterations * length; start_byte += read_size) {
            bytes_read += shared_buffer.sgetn(buffer, read_size);
        }
        shared_buffer.SetReadDone();
    };

    TimeLogger timer;
    auto writer_thread = std::thread(writer);
    auto reader_thread = std::thread(reader);
    reader_thread.join();
    writer_thread.join();
    SPDLOG_INFO("push copy both in {} seconds", timer.duration());

    SPDLOG_TRACE(shared_buffer.StateString("test"));
    CHECK_EQ(bytes_read, length * num_iterations);
}

TEST_CASE("TestLockingCopyBothSides")
{
    InitMemoryManager(100 * 1024 * 1024);

    // Tests push performance with a fast drain.
    auto shared_buffer = LockingConcurrentStreamBuf(64 * 1024, "test");
    constexpr auto length = 7;
    constexpr size_t num_iterations = kBigIterations;

    auto writer = [&]() {
        char buffer[32] = {0};

        for (size_t i = 0; i < num_iterations; ++i) {
            CHECK_EQ(shared_buffer.sputn(buffer, length), length);
        }
        shared_buffer.SetWriteDone();
    };

    size_t bytes_read = 0;
    auto reader = [&]() {
        // Not divisible by 7, but divides total read size.
        constexpr size_t read_size = 3200;
        char buffer[read_size] = {0};

        for (size_t start_byte = 0; start_byte < num_iterations * length; start_byte += read_size) {
            bytes_read += shared_buffer.sgetn(buffer, read_size);
        }
        shared_buffer.SetReadDone();
    };

    TimeLogger timer;
    auto writer_thread = std::thread(writer);
    auto reader_thread = std::thread(reader);
    reader_thread.join();
    writer_thread.join();
    SPDLOG_INFO("locking copy both in {} seconds", timer.duration());

    SPDLOG_TRACE(shared_buffer.StateString("test"));
    CHECK_EQ(bytes_read, length * num_iterations);
}

TEST_CASE("TestPushBufGetPadding")
{
    InitMemoryManager(100 * 1024 * 1024);

    // Tests push performance with a fast drain.
    auto shared_buffer = PushStreambuf(64 * 1024, "test");
    constexpr auto length = 7;
    constexpr size_t num_iterations = kSmallIterations;

    auto writer = [&]() { shared_buffer.SetWriteDoneWithPadding(num_iterations * length); };

    size_t bytes_read = 0;
    auto reader = [&]() {
        // Not divisible by 7, but divides total read size.
        constexpr size_t read_size = 3200;
        char buffer[read_size] = {0};

        for (size_t start_byte = 0; start_byte < num_iterations * length; start_byte += read_size) {
            bytes_read += shared_buffer.sgetn(buffer, read_size);
        }
        shared_buffer.SetReadDone();
    };

    TimeLogger timer;
    auto writer_thread = std::thread(writer);
    auto reader_thread = std::thread(reader);
    reader_thread.join();
    writer_thread.join();
    SPDLOG_INFO("push get padding in {} seconds", timer.duration());

    SPDLOG_TRACE(shared_buffer.StateString("test"));
    CHECK_EQ(bytes_read, length * num_iterations);
}

TEST_CASE("TestLockingGetPadding")
{
    InitMemoryManager(100 * 1024 * 1024);

    // Tests push performance with a fast drain.
    auto shared_buffer = LockingConcurrentStreamBuf(64 * 1024, "test");
    constexpr auto length = 7;
    constexpr size_t num_iterations = kSmallIterations;

    auto writer = [&]() { shared_buffer.SetWriteDoneWithPadding(num_iterations * length); };

    size_t bytes_read = 0;
    auto reader = [&]() {
        // Not divisible by 7, but divides total read size.
        constexpr size_t read_size = 3200;
        char buffer[read_size] = {0};

        for (size_t start_byte = 0; start_byte < num_iterations * length; start_byte += read_size) {
            bytes_read += shared_buffer.sgetn(buffer, read_size);
        }
        shared_buffer.SetReadDone();
    };

    TimeLogger timer;
    auto writer_thread = std::thread(writer);
    auto reader_thread = std::thread(reader);
    reader_thread.join();
    writer_thread.join();
    SPDLOG_INFO("locking get padding in {} seconds", timer.duration());

    SPDLOG_TRACE(shared_buffer.StateString("test"));
    CHECK_EQ(bytes_read, length * num_iterations);
}

TEST_CASE("TestPushBufGetTimeout")
{
    InitMemoryManager(100 * 1024 * 1024);

    // Tests push performance with a fast drain.
    auto shared_buffer = PushStreambuf(64 * 1024, "test", 1, 50, 0, 0, 0);
    constexpr size_t num_iterations = 500;

    auto writer = [&]() {
        char buffer[32] = {0};

        for (size_t i = 0; i < num_iterations; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
            if (shared_buffer.sputn(buffer, 1) == 0) {
                break;
            }
        }
    };

    auto reader = [&]() {
        char buffer[num_iterations] = {0};
        CHECK_LT(shared_buffer.sgetn(buffer, 200), 200);
        shared_buffer.SetReadDone();
    };

    TimeLogger timer;
    auto writer_thread = std::thread(writer);
    auto reader_thread = std::thread(reader);
    reader_thread.join();
    writer_thread.join();
    SPDLOG_INFO("push timeout test in {} seconds", timer.duration());

    SPDLOG_TRACE(shared_buffer.StateString("test"));
}

TEST_CASE("TestPushBufGetNoTimeout")
{
    InitMemoryManager(100 * 1024 * 1024);

    // Tests push performance with a fast drain.
    auto shared_buffer = PushStreambuf(64 * 1024, "test", 1, 400, 0, 0, 0);
    constexpr size_t num_iterations = 500;

    auto writer = [&]() {
        char buffer[32] = {0};

        for (size_t i = 0; i < num_iterations; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
            if (shared_buffer.sputn(buffer, 1) == 0) {
                break;
            }
        }
    };

    auto reader = [&]() {
        char buffer[num_iterations] = {0};
        CHECK_EQ(shared_buffer.sgetn(buffer, 200), 200);
        shared_buffer.SetReadDone();
    };

    TimeLogger timer;
    auto writer_thread = std::thread(writer);
    auto reader_thread = std::thread(reader);
    reader_thread.join();
    writer_thread.join();
    SPDLOG_INFO("push no timeout test in {} seconds", timer.duration());

    SPDLOG_TRACE(shared_buffer.StateString("test"));
}

TEST_CASE("TestPushBufNoGetLowWaterMark")
{
    InitMemoryManager(100 * 1024 * 1024);

    // Tests push performance with a fast drain.
    auto shared_buffer = PushStreambuf(64 * 1024, "test", 50, 50, 0, 0, 1);
    constexpr size_t num_iterations = 150;

    auto writer = [&]() {
        char buffer[32] = {0};

        for (size_t i = 0; i < num_iterations; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
            CHECK_EQ(shared_buffer.sputn(buffer, 1), 1);
        }
        shared_buffer.SetWriteDone();
    };

    auto reader = [&]() {
        char buffer[num_iterations] = {0};

        CHECK_EQ(shared_buffer.sgetn(buffer, 2), 2);
        std::this_thread::sleep_for(std::chrono::milliseconds{2 * num_iterations});
        CHECK_EQ(shared_buffer.sgetn(buffer, num_iterations), num_iterations - 2);
        shared_buffer.SetReadDone();
    };

    TimeLogger timer;
    auto writer_thread = std::thread(writer);
    auto reader_thread = std::thread(reader);
    reader_thread.join();
    writer_thread.join();
    SPDLOG_INFO("push lwm no get buffer in {} seconds", timer.duration());

    SPDLOG_TRACE(shared_buffer.StateString("test"));
}

TEST_CASE("TestPushBufGetLowWaterMark")
{
    InitMemoryManager(100 * 1024 * 1024);

    // Tests push performance with a fast drain.
    auto shared_buffer = PushStreambuf(64 * 1024, "test", 50, 50, 0, 0, 100);
    constexpr size_t num_iterations = 150;

    auto writer = [&]() {
        char buffer[32] = {0};

        for (size_t i = 0; i < num_iterations; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
            CHECK_EQ(shared_buffer.sputn(buffer, 1), 1);
        }
        shared_buffer.SetWriteDone();
    };

    auto reader = [&]() {
        char buffer[num_iterations] = {0};

        CHECK_EQ(shared_buffer.sgetn(buffer, 2), 1);
        std::this_thread::sleep_for(std::chrono::milliseconds{2 * num_iterations});
        CHECK_EQ(shared_buffer.sgetn(buffer, num_iterations), num_iterations - 1);
        shared_buffer.SetReadDone();
    };

    TimeLogger timer;
    auto writer_thread = std::thread(writer);
    auto reader_thread = std::thread(reader);
    reader_thread.join();
    writer_thread.join();
    SPDLOG_INFO("push lwm get buffer in {} seconds", timer.duration());

    SPDLOG_TRACE(shared_buffer.StateString("test"));
}

TEST_CASE("TestPushBufGetLowWaterMulti")
{
    InitMemoryManager(100 * 1024 * 1024);

    // Tests push performance with a fast drain.
    auto shared_buffer = PushStreambuf(64 * 1024, "test", 10, 10, 0, 0, 20);
    constexpr size_t num_iterations = 30;
    char buffer[num_iterations] = {0};

    auto reader = [&]() {
        CHECK_EQ(shared_buffer.sgetn(buffer, 10), 10);
        for (size_t i = 0; i < num_iterations - 10; ++i) {
            CHECK_EQ(shared_buffer.sgetn(buffer, num_iterations - 10 - i), 1);
        }
    };

    TimeLogger timer;
    CHECK_EQ(shared_buffer.sputn(buffer, num_iterations), num_iterations);
    auto reader_thread = std::thread(reader);
    reader_thread.join();
    SPDLOG_INFO("push lwm get buffer in {} seconds", timer.duration());

    SPDLOG_TRACE(shared_buffer.StateString("test"));
}

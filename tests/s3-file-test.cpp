#define USE_MOCK_S3 1

#include <random>

#define S3_INJECT_FILE_CACHE_SIZE 64

#include "s3_file.hpp"
#include "s3_utils.hpp"

#define CATCH_CONFIG_MAIN  // This tells Catch to provide a main() - only do this in one cpp file
#include "../lib/include/catch.hpp"

TEST_CASE("SequentialReadWrite")
{
    const auto filename = "test_file.bin";
    const size_t N = 100;

    for (auto stride : {13, 9, 16, 63}) {
        auto file = FileS3(filename, true, false, 0);
        for (size_t i = 0; i < N; ++i) {
            file.Write(stride * i, reinterpret_cast<uint8_t*>(&i), 8);
        }

        size_t actual_counter;
        for (size_t i = 0; i < N; ++i) {
            file.Read(stride * i, reinterpret_cast<uint8_t*>(&actual_counter), 8);
            CHECK_EQ(i, actual_counter) << "stride " << stride;
        }
        file.CloseAndDelete();
    }
}

TEST_CASE("RandomReadWrite")
{
    constexpr auto filename = "test_file.bin";
    const auto positions = {
        17,
        327,
        1,
        319,
        63,
        50,
        80,
        127,
        180,
    };

    auto file = FileS3(filename, true, false, 0);
    std::mt19937_64 gen(1337);
    for (auto pos : positions) {
        uint64_t value = gen();
        file.Write(pos, reinterpret_cast<uint8_t*>(&value), 8);
    }

    // Reset rng
    gen = std::mt19937_64(1337);
    for (auto pos : positions) {
        uint64_t expected_value = gen();
        uint64_t actual_value;
        file.Read(pos, reinterpret_cast<uint8_t*>(&actual_value), 8);
        CHECK_EQ(actual_value, expected_value) << "pos=" << pos;
    }

    file.CloseAndDelete();
}

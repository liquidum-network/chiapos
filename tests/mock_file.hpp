#ifndef TEST_CPP_MOCK_FILE_HPP_
#define TEST_CPP_MOCK_FILE_HPP_

#include <utility>

#define SRC_CPP_STORAGE_HPP_

#include "test_logging.hpp"

struct StorageContext {
    std::string local_filename;
};

struct MockFile {
    MockFile(uint8_t* buffer, size_t size, size_t read_size)
        : buffer(buffer), size(size), cursor(0), read_size(read_size)
    {
    }

    std::pair<uint8_t*, size_t> BorrowAligned()
    {
        CHECK_LE(cursor, size);

        const auto this_read_size = std::min(size - cursor, read_size);
        auto result = std::make_pair(buffer + cursor, this_read_size);
        cursor += this_read_size;
        return result;
    }

    void Close() {}

    StorageContext GetStorageContext()
    {
        return StorageContext{
            .local_filename = "mock_test",
        };
    }

    uint8_t* buffer;
    size_t size;
    size_t cursor;
    size_t read_size;
};

#endif  // TEST_CPP_MOCK_FILE_HPP_

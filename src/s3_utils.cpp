#include "s3_utils.hpp"

#include <fcntl.h>
#include <fmt/core.h>
#include <unistd.h>

#include <filesystem>
#include <string>

#include "logging.hpp"
#include "storage.hpp"

#if USE_MOCK_S3 == 1

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <climits>
#include <functional>
#include <thread>
#include <vector>

// TODO: Combine these (they handle missing files differently)
std::pair<int, bool> OpenMockS3File(const std::string& bucket, const std::string& key, bool write)
{
    const auto object_path = GetMockObjectPath(bucket, key);
    std::error_code ec;

    if (write) {
        std::filesystem::create_directories(object_path.parent_path(), ec);
        CHECK(
            !ec, "Could not create path {}: {}", object_path.parent_path().string(), ec.message());
    }

    const int fd = open(object_path.c_str(), write ? O_WRONLY | O_CREAT | O_TRUNC : O_RDONLY, 0600);
    if (fd < 0) {
        if (!write && errno == ENOENT) {
            return {-1, true};
        }
        CHECK(false, "couldn't open file: {}", ::strerror(errno));
    }
    return {fd, false};
}

std::pair<std::string, int> OpenFile(const std::filesystem::path& path, bool write)
{
    if (write) {
        std::error_code ec;
        std::filesystem::create_directories(path.parent_path(), ec);
        if (ec) {
            return {
                fmt::format(
                    "Could not create path {}: {}", path.parent_path().string(), ec.message()),
                -1};
        }
    }
    const int fd = open(path.c_str(), write ? O_WRONLY | O_CREAT | O_TRUNC : O_RDONLY, 0600);
    if (fd < 0) {
        return {fmt::format("couldn't open file {}: {}", path.string(), ::strerror(errno)), -1};
    }
    return {"", fd};
}

std::filesystem::path GetMockObjectPath(const std::string& bucket, const std::string& key)
{
    const auto cache_base_dir_path = std::filesystem::path(kMockTmpDir);
    return cache_base_dir_path / bucket / key;
}

std::string WriteBufferToS3(
    S3Client* s3_client_unused,
    const std::string& bucket,
    const std::string& key,
    const uint8_t* buffer,
    size_t buffer_size)
{
    const auto [fd, _] = OpenMockS3File(bucket, key, true);
    const ssize_t bytes_written_or_error = pwrite(fd, buffer, buffer_size, 0);
    std::string error_message;
    if (bytes_written_or_error != static_cast<ssize_t>(buffer_size)) {
        error_message = fmt::format(
            "Error writing to {}/{} +{}: pwrite returned {} {} ({})",
            bucket,
            key,
            buffer_size,
            bytes_written_or_error,
            ::strerror(errno),
            errno);
    }
    close(fd);

    return error_message;
}

#else  // USE_MOCK_S3 == 1

#if USE_AWS_CRT == 1
#include "aws/s3-crt/model/GetObjectRequest.h"
#include "aws/s3-crt/model/PutObjectRequest.h"
#else
#include "aws/s3/model/GetObjectRequest.h"
#include "aws/s3/model/PutObjectRequest.h"
#endif

std::string WriteBufferToS3(
    S3Client* s3_client,
    const std::string& bucket,
    const std::string& key,
    const uint8_t* buffer,
    size_t buffer_size)
{
    auto request = s3::Model::PutObjectRequest().WithBucket(bucket).WithKey(key).WithContentLength(
        buffer_size);
    request.SetBody(std::make_shared<StringViewStream>(buffer, buffer_size));
    const auto outcome = s3_client->PutObject(request);

    if (!outcome.IsSuccess()) {
        // TODO: Retry with long delay
        const auto error_message = fmt::format(
            "failed to write {}/{} size {}: {} ({}-{}), {}",
            bucket,
            key,
            buffer_size,
            outcome.GetError().GetExceptionName(),
            outcome.GetError().GetResponseCode(),
            outcome.GetError().GetErrorType(),
            outcome.GetError().GetMessage());
        return error_message;
    }

    return "";
}

#endif  // USE_MOCK_S3 == 1

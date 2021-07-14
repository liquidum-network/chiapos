#include "s3_read.hpp"

#if USE_MOCK_S3 == 1

#include <fcntl.h>
#include <unistd.h>

std::pair<std::string, size_t> ReadBufferFromS3(
    S3Client* s3_client_unused,
    const std::string& bucket,
    const std::string& key,
    uint8_t* buffer,
    ssize_t offset,
    size_t buffer_size)
{
    // Sometimes files we have written are missing for a while.
    std::pair<int, bool> pair;
    for (int i = 0; i < 3; ++i) {
        pair = OpenMockS3File(bucket, key, false);
        if (!pair.second) {
            break;
        }

        SPDLOG_INFO("Missing file {}, sleeping", key);
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
    }

    if (pair.second) {
        // Empty file.
        return {"", 0};
    }

    int fd = pair.first;
    const ssize_t bytes_read_or_error = pread(fd, buffer, buffer_size, std::max(0L, offset));
    if (bytes_read_or_error < 0) {
        return {
            fmt::format(
                "Error reading from {}/{} @{}+{}: pread returned {} {} ({})",
                bucket,
                key,
                std::max(0L, offset),
                buffer_size,
                bytes_read_or_error,
                ::strerror(errno),
                errno),
            0};
    }

    close(fd);
    return {"", static_cast<size_t>(bytes_read_or_error)};
}

#else  // USE_MOCK_S3 == 1

std::pair<std::string, size_t> ReadBufferFromS3(
    S3Client* s3_client,
    const std::string& bucket,
    const std::string& key,
    uint8_t* buffer,
    ssize_t offset,
    size_t buffer_size)
{
    auto request = s3::Model::GetObjectRequest().WithBucket(bucket).WithKey(key);
    if (offset >= 0) {
        request.SetRange(GetHttpRange(offset, buffer_size));
    }
    request.SetResponseStreamFactory(AwsWriteableStreamFactory(buffer, buffer_size));
    const auto outcome = s3_client->GetObject(request);

    if (!outcome.IsSuccess()) {
        if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
            // This is a block we haven't written to yet.
            // Not an error.
            return {"", 0};
        }

        // TODO: Retry with long delay
        const auto error_message = fmt::format(
            "failed to read {}/{} at {} size {}: {} ({}-{}), {}",
            request.GetBucket(),
            request.GetKey(),
            std::max(0L, offset),
            buffer_size,
            std::string(outcome.GetError().GetExceptionName()),
            outcome.GetError().GetResponseCode(),
            outcome.GetError().GetErrorType(),
            std::string(outcome.GetError().GetMessage()));
        return {error_message, 0};
    }

    const auto bytes_read = outcome.GetResult().GetContentLength();
    return {"", bytes_read};
}

#endif  // USE_MOCK_S3 == 1

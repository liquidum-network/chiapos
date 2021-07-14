#ifndef SRC_CPP_S3_READ_HPP_
#define SRC_CPP_S3_READ_HPP_

#include "s3_plotter.hpp"
#include "s3_utils.hpp"

std::pair<std::string, size_t> ReadBufferFromS3(
    S3Client* s3_client,
    const std::string& bucket,
    const std::string& key,
    uint8_t* buffer,
    ssize_t offset,
    size_t read_size);

#if USE_MOCK_S3 == 1

#include <chrono>
#include <random>

template <typename TFunc>
void GenericReadFromS3Async(
    S3Client* s3_client,
    const std::string& bucket,
    const std::string& key,
    uint8_t* dest,
    ssize_t offset,
    size_t read_size,
    TFunc callback)
{
    auto await_read =
        [s3_client, bucket, key, dest, offset, read_size, callback = std::move(callback)]() {
            auto [error_message, bytes_read] =
                ReadBufferFromS3(s3_client, bucket, key, dest, offset, read_size);

            mock_s3_sleep_exponential();
            callback(bytes_read, std::move(error_message));
        };

    s3_plotter::shared_thread_pool_->SubmitTask(std::move(await_read));
}

#else  // USE_MOCK_S3

template <typename TFunc>
void GenericReadFromS3Async(
    S3Client* s3_client,
    const std::string& bucket,
    const std::string& key,
    uint8_t* dest,
    ssize_t offset,
    size_t read_size,
    TFunc callback)
{
    auto wrapped_callback =
        [read_size, callback = std::move(callback)](
            const S3Client* client,
            const s3::Model::GetObjectRequest& request,
            s3::Model::GetObjectOutcome outcome,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext>& parent_context) mutable {
            if (!outcome.IsSuccess()) {
                const auto error_message = fmt::format(
                    "failed to read {}/{}: {}, {}",
                    request.GetBucket(),
                    request.GetKey(),
                    outcome.GetError().GetExceptionName(),
                    outcome.GetError().GetMessage());
                return callback({}, error_message);
            }

            const auto bytes_read = outcome.GetResult().GetContentLength();
            if (bytes_read != static_cast<ssize_t>(read_size)) {
                return callback(
                    {},
                    fmt::format(
                        "Read too few bytes from {}/{}: {} < {}",
                        request.GetBucket(),
                        request.GetKey(),
                        bytes_read,
                        read_size));
            }
            return callback(bytes_read, "");
        };

    auto request = s3::Model::GetObjectRequest().WithBucket(bucket).WithKey(key);
    if (offset >= 0) {
        request.SetRange(GetHttpRange(offset, read_size));
    }
    request.SetResponseStreamFactory(AwsWriteableStreamFactory(dest, read_size));
    s3_plotter::shared_client_->GetObjectAsync(request, wrapped_callback, nullptr);
}

#endif  // USE_MOCK_S3
#endif  // SRC_CPP_S3_READ_HPP_

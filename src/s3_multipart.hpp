#ifndef SRC_CPP_S3_MULTIPART_HPP_
#define SRC_CPP_S3_MULTIPART_HPP_

#include <string>

#include "storage.hpp"

struct MultipartUploadId {
    std::string id;
};

#if USE_MOCK_S3 == 1

#include <fcntl.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <climits>
#include <functional>
#include <random>
#include <thread>
#include <vector>

#include "serialization.hpp"

namespace {

std::filesystem::path GetMultipartPath(
    const std::string& bucket,
    const MultipartUploadId& upload_id,
    size_t part_number)
{
    const auto cache_base_dir_path = std::filesystem::path("/tmp/mock-s3/");
    return cache_base_dir_path / bucket / "mock-uploads" / upload_id.id /
           std::to_string(part_number);
}
}  // namespace

inline std::pair<std::string, MultipartUploadId> CreateMultipartUpload(
    S3Client* s3_client,
    const StorageContext& storage_context)
{
    using random_bytes_engine = std::independent_bits_engine<std::mt19937, 8, uint64_t>;
    random_bytes_engine rbe{std::random_device{}()};
    std::vector<uint8_t> data(4);
    std::generate(std::begin(data), std::end(data), std::ref(rbe));

    const auto upload_id = MultipartUploadId{BytesToHex(data)};
    const auto part_dir = GetMultipartPath(storage_context.bucket, upload_id, 1).parent_path();

    std::error_code ec;
    std::filesystem::create_directories(part_dir, ec);

    if (ec != std::error_code()) {
        return {ec.message(), {}};
    }
    SPDLOG_DEBUG("create multipart {} -> {}", storage_context.GetS3Key(), upload_id.id);

    return {"", upload_id};
}

template <typename ETagContainer>
std::string CompleteMultipartUpload(
    S3Client* s3_client_unused,
    const StorageContext& storage_context,
    const MultipartUploadId& upload_id,
    const ETagContainer& parts)
{
    // Move the parts into place.
    const auto cache_base_dir_path = std::filesystem::path("/tmp/mock-s3/");
    const auto result_path =
        cache_base_dir_path / storage_context.bucket / storage_context.GetS3Key();
    std::filesystem::create_directories(result_path.parent_path());

    auto [open_file_error_message, write_fd] = OpenFile(result_path, true);
    if (!open_file_error_message.empty()) {
        return open_file_error_message;
    }

    size_t part_number = 1;
    for (const auto& _ : parts) {
        (void)_;
        const auto part_path = GetMultipartPath(storage_context.bucket, upload_id, part_number);
        auto [error_message, read_fd] = OpenFile(part_path, false);
        if (!error_message.empty()) {
            close(write_fd);
            return error_message;
        }

        ssize_t part_size = lseek(read_fd, 0, SEEK_END);
        lseek(read_fd, 0, SEEK_SET);
        SPDLOG_TRACE("sending file from {} to {} size {}", part_path, result_path, part_size);
        ssize_t offset = 0;
        while (offset < part_size) {
            const ssize_t bytes_written_or_error =
                sendfile(write_fd, read_fd, &offset, part_size - offset);
            if (bytes_written_or_error <= 0) {
                close(write_fd);
                close(read_fd);
                return fmt::format(
                    "Error writing part {} +{}: sendfile returned {} {} ({})",
                    result_path.string(),
                    part_size,
                    bytes_written_or_error,
                    ::strerror(errno),
                    errno);
            }
        }
        close(read_fd);
        part_number += 1;
    }
    SPDLOG_DEBUG(
        "complete multipart {} -> {} size {} parts",
        storage_context.GetS3Key(),
        upload_id.id,
        parts.size());

    std::filesystem::remove_all(
        GetMultipartPath(storage_context.bucket, upload_id, 1).parent_path());

    close(write_fd);
    return "";
}

inline std::string AbortMultipartUpload(
    S3Client* s3_client_unused,
    const StorageContext& storage_context,
    const MultipartUploadId& upload_id)
{
    const auto parts_path = GetMultipartPath(storage_context.bucket, upload_id, 1).parent_path();
    std::filesystem::remove_all(parts_path);
    return "";
}

#else  // USE_MOCK_S3 == 1

#if USE_AWS_CRT == 1
#include "aws/s3-crt/model/AbortMultipartUploadRequest.h"
#include "aws/s3-crt/model/CompleteMultipartUploadRequest.h"
#include "aws/s3-crt/model/CompletedMultipartUpload.h"
#include "aws/s3-crt/model/CompletedPart.h"
#include "aws/s3-crt/model/CreateMultipartUploadRequest.h"
#include "aws/s3-crt/model/UploadPartRequest.h"
#else
#include "aws/s3/model/AbortMultipartUploadRequest.h"
#include "aws/s3/model/CompleteMultipartUploadRequest.h"
#include "aws/s3/model/CompletedMultipartUpload.h"
#include "aws/s3/model/CompletedPart.h"
#include "aws/s3/model/CreateMultipartUploadRequest.h"
#include "aws/s3/model/UploadPartRequest.h"
#endif

inline std::pair<std::string, MultipartUploadId> CreateMultipartUpload(
    S3Client* s3_client,
    const StorageContext& storage_context)
{
    auto request = s3::Model::CreateMultipartUploadRequest()
                       .WithBucket(storage_context.bucket)
                       .WithKey(storage_context.GetS3Key())
                       .WithStorageClass(
                           storage_context.use_cold_storage_class ? StorageClass::ONEZONE_IA
                                                                  : StorageClass::STANDARD);
    auto outcome = s3_client->CreateMultipartUpload(request);
    if (!outcome.IsSuccess()) {
        const auto error_message = fmt::format(
            "could not create multipart upload {}/{}: {}, {}",
            request.GetBucket(),
            request.GetKey(),
            outcome.GetError().GetExceptionName(),
            outcome.GetError().GetMessage());
    }

    return {"", MultipartUploadId{.id = outcome.GetResult().GetUploadId()}};
}

template <typename ETagContainer>
std::string CompleteMultipartUpload(
    S3Client* s3_client,
    const StorageContext& storage_context,
    const MultipartUploadId& upload_id,
    const ETagContainer& parts)
{
    std::vector<s3::Model::CompletedPart> completed_parts;
    size_t part_number = 1;
    for (const auto& part : parts) {
        completed_parts.push_back(
            s3::Model::CompletedPart().WithPartNumber(part_number).WithETag(part->etag));
        part_number += 1;
    }

    auto request = s3::Model::CompleteMultipartUploadRequest()
                       .WithBucket(storage_context.bucket)
                       .WithKey(storage_context.GetS3Key())
                       .WithUploadId(upload_id.id)
                       .WithMultipartUpload(s3::Model::CompletedMultipartUpload().WithParts(
                           std::move(completed_parts)));

    auto outcome = s3_client->CompleteMultipartUpload(request);
    if (!outcome.IsSuccess()) {
        return fmt::format(
            "could not complete multipart upload {}/{}: {}, {}",
            request.GetBucket(),
            request.GetKey(),
            outcome.GetError().GetExceptionName(),
            outcome.GetError().GetMessage());
    }

    SPDLOG_DEBUG(
        "complete multipart {} -> {} size {} parts",
        request.GetKey(),
        request.GetUploadId(),
        parts.size());

    return "";
}

inline std::string AbortMultipartUpload(
    S3Client* s3_client,
    const StorageContext& storage_context,
    const MultipartUploadId& upload_id)
{
    auto request = s3::Model::AbortMultipartUploadRequest()
                       .WithBucket(storage_context.bucket)
                       .WithKey(storage_context.GetS3Key())
                       .WithUploadId(upload_id.id);
    auto outcome = s3_client->AbortMultipartUpload(request);
    if (!outcome.IsSuccess()) {
        return fmt::format(
            "could not abort multipart upload {}/{}: {}, {}",
            request.GetKey(),
            outcome.GetError().GetExceptionName(),
            outcome.GetError().GetMessage());
    }

    SPDLOG_DEBUG("abort multipart {} -> {}", request.GetKey(), request.GetUploadId());

    return "";
}

#endif  // USE_MOCK_S3 == 1
#endif  // SRC_CPP_S3_MULTIPART_HPP_

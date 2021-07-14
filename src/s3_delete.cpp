#include "s3_delete.hpp"

#include "aws_async.hpp"
#include "s3_plotter.hpp"

#if USE_MOCK_S3 == 1

#include <filesystem>

void RemoveS3ObjectIgnoreResult(
    S3Client* s3_client_unused,
    const std::string& bucket,
    const std::string& key)
{
    CHECK_NE(bucket, "");
    const auto object_path = GetMockObjectPath(bucket, key);
    auto remove_task = [object_path]() { std::filesystem::remove(object_path); };

    s3_plotter::shared_thread_pool_->SubmitTask(
        WrapCallbackAwaitOnShutdown(std::move(remove_task)));
}

void RemoveS3ObjectTree(S3Client* s3_client, const std::string& bucket, const std::string& prefix)
{
    auto remove_task = [prefix_path = GetMockObjectPath(bucket, prefix)]() {
        std::filesystem::remove_all(prefix_path);
    };

    s3_plotter::shared_thread_pool_->SubmitTask(std::move(remove_task));
}
#else  // USE_MOCK_S3

#if USE_AWS_CRT == 1
#include "aws/s3-crt/model/DeleteObjectRequest.h"
#else
#include "aws/s3/model/DeleteObjectRequest.h"
#endif

void RemoveS3ObjectIgnoreResult(
    S3Client* s3_client,
    const std::string& bucket,
    const std::string& key)
{
    auto callback =
        [bucket, key](
            const S3Client* client,
            const s3::Model::DeleteObjectRequest& request,
            s3::Model::DeleteObjectOutcome outcome,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext>& parent_context) mutable {
            if (!outcome.IsSuccess()) {
                SPDLOG_ERROR(fmt::format(
                    "failed to delete {}/{}: {}, {}",
                    request.GetBucket(),
                    request.GetKey(),
                    outcome.GetError().GetExceptionName(),
                    outcome.GetError().GetMessage()));
            }
        };

    auto request = s3::Model::DeleteObjectRequest().WithBucket(bucket).WithKey(key);
    s3_client->DeleteObjectAsync(
        request, WrapCallbackAwaitOnShutdown(std::move(callback)), nullptr);
}

void RemoveS3ObjectTree(S3Client* s3_client, const std::string& bucket, const std::string& prefix)
{
    // TODO Implement
    // SPDLOG_DEBUG("not implemented: RemoveS3ObjectTree");
}

#endif  // USE_MOCK_S3

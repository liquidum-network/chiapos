#include "s3_sync_read_file.hpp"

#include "buffers.hpp"
#include "s3_delete.hpp"
#include "s3_read.hpp"
#include "s3_utils.hpp"
#include "util.hpp"

namespace s3_plotter {
namespace {

void FinishRead(std::string error_message, ReadState* state)
{
    if (!error_message.empty()) {
        SPDLOG_ERROR("{} reading: {}", state->key, error_message);
    }
    state->in_buffer.SetWriteDone();

    state->finish(std::move(error_message));
}

}  // namespace
}  // namespace s3_plotter

#if USE_MOCK_S3 == 1

#include <fcntl.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <random>
#include <thread>

namespace s3_plotter {
namespace {

void ReadAsync(
    S3Client* s3_client,
    const std::string& bucket,
    const std::string& key,
    size_t size,
    std::shared_ptr<ReadState> state)
{
    auto await_done = [s3_client, bucket, key, size, state = std::move(state)]() {
        auto& local_state = *state;

        const auto path = GetMockObjectPath(bucket, key);
        const auto [error_message, in_fd] = OpenFile(path, false);

        if (!error_message.empty()) {
            FinishRead(std::move(error_message), &local_state);
            return;
        }

        SPDLOG_TRACE("starting read of {}", key);

        ssize_t read_pos = 0;
        while (true) {
            mock_s3_sleep_exponential();

            SPDLOG_TRACE("{} putting for read", key);
            const auto [ok, write_ptr, write_size] = local_state.in_buffer.BorrowPutBytes(1);
            if (!ok) {
                FinishRead("timeout", &local_state);
                return;
            }
            CHECK_NE(write_ptr, nullptr);
            const auto read_size = std::max(0UL, std::min(write_size, size - read_pos));
            const ssize_t bytes_read_or_error = pread(in_fd, write_ptr, read_size, read_pos);
            if (bytes_read_or_error == 0) {
                FinishRead("", &local_state);
                break;
            }
            if (bytes_read_or_error < 0) {
                FinishRead(
                    fmt::format(
                        "Error reading part {} +{}: pread returned {} {} ({})",
                        path.string(),
                        read_size,
                        bytes_read_or_error,
                        ::strerror(errno),
                        errno),
                    &local_state);
                break;
            }
            local_state.in_buffer.FinishPutBytes(bytes_read_or_error);
            read_pos += bytes_read_or_error;
            SPDLOG_TRACE("{} done putting for read {} bytes", key, bytes_read_or_error);
        }

        SPDLOG_TRACE("done read of {}", key);
        close(in_fd);
    };

    shared_thread_pool_->SubmitTask(std::move(await_done));
}

}  // namespace
}  // namespace s3_plotter

#else  // USE_MOCK_S3 == 1

namespace s3_plotter {
namespace {

void GetObjectCallback(
    const S3Client* client,
    const s3::Model::GetObjectRequest& request,
    s3::Model::GetObjectOutcome outcome,
    const std::shared_ptr<const Aws::Client::AsyncCallerContext>& parent_context)
{
    auto* state_ptr =
        const_cast<ReadState*>(reinterpret_cast<const ReadState*>(parent_context.get()));

    if (!outcome.IsSuccess()) {
        FinishRead(
            fmt::format(
                "failed to read {}/{} size {}: {} ({}-{}), {}",
                request.GetBucket(),
                request.GetKey(),
                outcome.GetResult().GetContentLength(),
                outcome.GetError().GetExceptionName(),
                outcome.GetError().GetResponseCode(),
                outcome.GetError().GetErrorType(),
                outcome.GetError().GetMessage()),
            state_ptr);
        return;
    }

    SPDLOG_TRACE("read {} {} bytes", request.GetKey(), outcome.GetResult().GetContentLength());
    FinishRead("", state_ptr);
}

void ReadAsync(
    S3Client* s3_client,
    const std::string& bucket,
    const std::string& key,
    size_t size,
    std::shared_ptr<ReadState> state)
{
    auto request = s3::Model::GetObjectRequest().WithBucket(bucket).WithKey(key).WithRange(
        GetHttpRange(0, size));
    request.SetResponseStreamFactory(
        [=]() { return Aws::New<std::iostream>("plotter", &state->in_buffer); });

#if USE_MOCK_S3 != 1 && USE_AWS_CRT == 1
    state->handler_container_for_crt_bug = GetObjectCallback;
    s3_plotter::shared_client_->GetObjectAsync(
        request, state->handler_container_for_crt_bug, state);
#else   // USE_MOCK_S3 != 1 && USE_AWS_CRT == 1
    s3_plotter::shared_client_->GetObjectAsync(request, GetObjectCallback, state);
#endif  // USE_MOCK_S3 != 1 && USE_AWS_CRT == 1
}

}  // namespace
}  // namespace s3_plotter

#endif  // USE_MOCK_S3 == 1

void S3SyncReadFile::CloseAndDelete()
{
    Close();
    if (file_size_ > 0) {
        RemoveS3ObjectIgnoreResult(s3_plotter::shared_client_.get(), bucket(), key());
    }
}

std::string S3SyncReadFile::ReadEntireFile(uint8_t* dest, size_t read_size, uint64_t timeout_millis)
{
    auto [error_message, bytes_read] =
        ReadBufferFromS3(s3_plotter::shared_client_.get(), bucket(), key(), dest, 0, read_size);
    if (!error_message.empty()) {
        return error_message;
    }
    CHECK_EQ(bytes_read, read_size);
    return "";
}

std::shared_ptr<s3_plotter::ReadState> S3SyncReadFile::StartRead()
{
    CHECK_GT(file_size_, 0);
    auto state = std::make_shared<s3_plotter::ReadState>(
        key(), buffers::GetClosestAlignedToTarget(alignment_, buffers::kTargetReadBufferSize));
    s3_plotter::ReadAsync(s3_plotter::shared_client_.get(), bucket(), key(), file_size_, state);
    return state;
}

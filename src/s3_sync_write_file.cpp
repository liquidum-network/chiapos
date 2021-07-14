#include "s3_sync_write_file.hpp"

#include "memstream.hpp"
#include "s3_plotter.hpp"
#include "s3_utils.hpp"

namespace s3_plotter {
namespace {
void FinishWrite(std::string error_message, WriteState* state)
{
    if (!error_message.empty()) {
        CHECK(false, "{} writing: {}", state->out_buffer.id(), error_message);
    }
    state->out_buffer.SetReadDone();

    // TODO: Free buffer here.
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
#include <filesystem>
#include <random>
#include <thread>

namespace s3_plotter {
namespace {
void WriteAsync(
    S3Client* s3_client,
    const StorageContext& storage_context,
    size_t content_length_size,
    std::shared_ptr<WriteState> state)
{
    auto await_write_done = [s3_client,
                             bucket = storage_context.bucket,
                             key = storage_context..GetS3Key(),
                             content_length_size,
                             state = std::move(state)]() {
        auto& local_state = *state;

        const auto path = GetMockObjectPath(bucket, key);
        std::filesystem::create_directories(path.parent_path());
        const auto [error_message, out_fd] = OpenFile(path, true);

        if (!error_message.empty()) {
            FinishWrite(std::move(error_message), &local_state);
            return;
        }

        size_t write_pos = 0;
        while (true) {
            mock_s3_sleep_exponential();

            const auto [ok, read_ptr, read_size] = local_state.out_buffer.BorrowGetBytes(1);
            if (!ok) {
                FinishWrite("timeout", &local_state);
                break;
            }
            CHECK_NE(read_ptr, nullptr);

            if (read_size == 0) {
                CHECK(local_state.out_buffer.write_done());
                SPDLOG_TRACE("breaking eof for {}", key);
                FinishWrite("", &local_state);
                break;
            }

            const ssize_t bytes_written_or_error = pwrite(out_fd, read_ptr, read_size, write_pos);
            if (bytes_written_or_error != static_cast<ssize_t>(read_size)) {
                FinishWrite(
                    fmt::format(
                        "Error writing part {} +{}: pwrite returned {} {} ({})",
                        path.string(),
                        read_size,
                        bytes_written_or_error,
                        ::strerror(errno),
                        errno),
                    &local_state);
                break;
            }
            write_pos += bytes_written_or_error;
            local_state.out_buffer.FinishGetBytes(read_size);
        }

        close(out_fd);
        CHECK_EQ(write_pos, content_length_size);
    };

    shared_thread_pool_->SubmitTask(std::move(await_write_done));
}
}  // namespace
}  // namespace s3_plotter

#else  // USE_MOCK_S3 == 1

namespace s3_plotter {
namespace {

void PutObjectCallback(
    const S3Client* client,
    const s3::Model::PutObjectRequest& request,
    s3::Model::PutObjectOutcome outcome,
    const std::shared_ptr<const Aws::Client::AsyncCallerContext>& parent_context)
{
    auto* state_ptr =
        const_cast<WriteState*>(reinterpret_cast<const WriteState*>(parent_context.get()));

    if (!outcome.IsSuccess()) {
        FinishWrite(
            fmt::format(
                "failed to write {}/{} size {}: {} ({}-{}), {}",
                request.GetBucket(),
                request.GetKey(),
                request.GetContentLength(),
                outcome.GetError().GetExceptionName(),
                outcome.GetError().GetResponseCode(),
                outcome.GetError().GetErrorType(),
                outcome.GetError().GetMessage()),
            state_ptr);
        return;
    }

    SPDLOG_TRACE("wrote {}", request.GetKey());
    FinishWrite("", state_ptr);
}

void WriteAsync(
    S3Client* s3_client,
    const StorageContext& storage_context,
    size_t content_length_size,
    std::shared_ptr<WriteState> state)
{
    auto request = s3::Model::PutObjectRequest()
                       .WithBucket(storage_context.bucket)
                       .WithKey(storage_context.GetS3Key())
                       .WithContentLength(content_length_size)
                       .WithStorageClass(
                           storage_context.use_cold_storage_class ? StorageClass::ONEZONE_IA
                                                                  : StorageClass::STANDARD);
    request.SetBody(std::make_shared<std::iostream>(&state->out_buffer));

#if USE_MOCK_S3 != 1 && USE_AWS_CRT == 1
    state->handler_container_for_crt_bug = PutObjectCallback;
    s3_plotter::shared_client_->PutObjectAsync(
        request, state->handler_container_for_crt_bug, state);
#else   // USE_MOCK_S3 != 1 && USE_AWS_CRT == 1
    s3_plotter::shared_client_->PutObjectAsync(request, PutObjectCallback, state);
#endif  // USE_MOCK_S3 != 1 && USE_AWS_CRT == 1
}
}  // namespace
}  // namespace s3_plotter

#endif  // USE_MOCK_S3 == 1

std::shared_ptr<s3_plotter::WriteState> S3SyncWriteFile::StartWrite()
{
    auto state = std::make_shared<s3_plotter::WriteState>(
        key(), buffers::GetClosestAlignedToTarget(alignment_, buffers::kTargetWriteBufferSize));
    s3_plotter::WriteAsync(
        s3_plotter::shared_client_.get(), GetStorageContext(), content_length_size_, state);
    return state;
}

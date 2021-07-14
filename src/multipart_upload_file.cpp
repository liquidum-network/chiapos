#include "multipart_upload_file.hpp"

#include <filesystem>
#include <iterator>

#include "s3_multipart.hpp"
#include "s3_plotter.hpp"

namespace {
void FinishUploadPart(
    std::string error_message,
    std::string etag,
    s3_plotter::UploadPartState* state)
{
    if (error_message.empty()) {
        SPDLOG_TRACE(
            "done part write {} part {} of size {}",
            state->out_buffer.id(),
            std::to_string(state->part_number),
            state->size);
    } else {
        SPDLOG_ERROR("{} uploading part: {}", state->out_buffer.id(), error_message);
    }

    // TODO: md5 etag verification.
    state->etag = std::move(etag);

    state->finish(std::move(error_message));
}

}  // namespace

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

void MultipartUploadFile::UploadPartAsync(std::shared_ptr<s3_plotter::UploadPartState> state)
{
    DCHECK(!upload_id_.id.empty());

    auto await_part_written = [bucket = bucket(),
                               upload_id = upload_id(),
                               state = std::move(state)]() {
        auto& local_state = *state;

        const auto path = GetMultipartPath(bucket, upload_id, local_state.part_number);
        std::filesystem::create_directories(path.parent_path());
        auto [error_message, out_fd] = OpenFile(path, true);

        if (!error_message.empty()) {
            FinishUploadPart(std::move(error_message), "", &local_state);
            return;
        }

        auto etag = std::to_string(local_state.part_number);

        size_t write_pos = 0;
        while (true) {
            mock_s3_sleep_exponential();

            const auto [ok, read_ptr, read_size] = local_state.out_buffer.BorrowGetBytes(1);
            if (!ok) {
                FinishUploadPart("timeout", "", &local_state);
                close(out_fd);
                return;
            }
            CHECK_NE(read_ptr, nullptr);

            if (read_size == 0) {
                CHECK(local_state.out_buffer.write_done());
                SPDLOG_TRACE("breaking eof for {}", key);
                FinishUploadPart("", std::move(etag), &local_state);
                close(out_fd);
                CHECK_EQ(write_pos, local_state.size);
                return;
            }

            const ssize_t bytes_written_or_error = pwrite(out_fd, read_ptr, read_size, write_pos);
            if (bytes_written_or_error != static_cast<ssize_t>(read_size)) {
                FinishUploadPart(
                    fmt::format(
                        "Error writing part {} +{}: pwrite returned {} {} ({})",
                        path.string(),
                        read_size,
                        bytes_written_or_error,
                        ::strerror(errno),
                        errno),
                    "",
                    &local_state);
                close(out_fd);
                return;
            }
            write_pos += bytes_written_or_error;
            local_state.out_buffer.FinishGetBytes(read_size);
        }
    };

    s3_plotter::shared_thread_pool_->SubmitTask(std::move(await_part_written));
}

#else  // USE_MOCK_S3 == 1

namespace {

void UploadPartCallback(
    const S3Client* client,
    const s3::Model::UploadPartRequest& request,
    s3::Model::UploadPartOutcome outcome,
    const std::shared_ptr<const Aws::Client::AsyncCallerContext>& parent_context)
{
    auto& state = *static_cast<s3_plotter::UploadPartState*>(
        const_cast<Aws::Client::AsyncCallerContext*>(parent_context.get()));

    std::string error_message;
    if (!outcome.IsSuccess()) {
        error_message = fmt::format(
            "failed to write {}/{} size {}: {} ({}-{}), {}",
            request.GetBucket(),
            request.GetUploadId(),
            request.GetContentLength(),
            outcome.GetError().GetExceptionName(),
            outcome.GetError().GetResponseCode(),
            outcome.GetError().GetErrorType(),
            outcome.GetError().GetMessage());
    }

    FinishUploadPart(std::move(error_message), outcome.GetResult().GetETag(), &state);
}

}  // namespace

void MultipartUploadFile::UploadPartAsync(std::shared_ptr<s3_plotter::UploadPartState> state)
{
    auto& local_state = *state;
    auto request = s3::Model::UploadPartRequest()
                       .WithBucket(bucket())
                       .WithKey(key())
                       .WithPartNumber(local_state.part_number)
                       .WithUploadId(upload_id().id)
                       .WithContentLength(local_state.size);
    request.SetBody(std::make_shared<std::iostream>(&state->out_buffer));
    s3_plotter::shared_client_->UploadPartAsync(request, UploadPartCallback, std::move(state));
}

#endif  // USE_MOCK_S3 == 1

std::string MultipartUploadFile::WaitForParts()
{
    const auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(3600);
    std::string error_message;
    for (auto& part_ptr : pending_part_uploads_) {
        auto& part = *part_ptr;
        if (!part.done()) {
            std::unique_lock<std::mutex> lock(part.mutex);
            // wait for done.
            SPDLOG_DEBUG(
                "waiting for multipart {} part {}",
                storage_context_.local_filename,
                part.part_number);
            if (!part.cv.wait_until(lock, deadline, [&part]() { return part.done(); })) {
                return "timeout waiting for part " + std::to_string(part.part_number);
            }
        }
        if (!part.error_message.empty()) {
            return part.error_message;
        }
    }

    return "";
}

size_t MultipartUploadFile::FinishCurrentPart()
{
    if (!current_part_state_) {
        return write_end_;
    }

    const auto end = this_part_end();

    // S3 requires a content length for PutObject, so we have to
    // overestimate the size of the file and write out random shit to
    // fill in at the end.
    current_part_state_->out_buffer.SetWriteDoneWithPadding(end - write_end_);
    pending_part_uploads_.emplace_back(std::move(current_part_state_));
    return end;
}

void MultipartUploadFile::StartNextPart()
{
    DCHECK(!upload_id_.id.empty());

    // start the next part
    if (write_end_ > 0) {
        CHECK_EQ(write_end_, part_index() * part_size_);
    }
    CHECK_LT(write_end_, content_length_size_);
    auto this_part_size = std::min(part_size_, content_length_size_ - write_end_);

    auto state = std::make_shared<s3_plotter::UploadPartState>(
        key(),
        buffer_size_,
        this_part_size,
        // AWS Part numbers start at 1.
        part_index() + 1,
        storage_context_.local_filename);

    UploadPartAsync(state);
    current_part_state_ = std::move(state);
}

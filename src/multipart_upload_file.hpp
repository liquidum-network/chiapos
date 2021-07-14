#ifndef SRC_CPP_MULTIPART_UPLOAD_FILE_HPP_
#define SRC_CPP_MULTIPART_UPLOAD_FILE_HPP_

#include <condition_variable>
#include <cstring>
#include <functional>
#include <map>
#include <mutex>

#include "aws_async.hpp"
#include "buffers.hpp"
#include "disk.hpp"
#include "logging_helpers.hpp"
#include "push_streambuf.hpp"
#include "s3_multipart.hpp"
#include "s3_plotter.hpp"
#include "s3_utils.hpp"
#include "serialization.hpp"
#include "storage.hpp"
#include "write_file.hpp"

namespace s3_plotter {

namespace {
size_t GetPartSizeForAlignment(size_t alignment)
{
    constexpr size_t size_limit = 5ULL * 1024 * 1024 * 1024;

    return buffers::GetLargestUnderLimit(alignment, size_limit);
}
}  // namespace

struct UploadPartState : public AsyncState {
    UploadPartState(
        std::string key,
        size_t buffer_size,
        size_t size,
        size_t part_number,
        const std::string &buffer_id)
        : out_buffer(buffer_size, buffer_id, 40 * 1000, 10 * 1000, 32 * 1024, 0, 32),
          size(size),
          part_number(part_number)
    {
    }

    void WriteArbitrary(size_t length)
    {
        auto remaining_length = length;
        while (remaining_length > 0) {
            // Writes whatever happens to be in the buffer.
            auto [ok, __, write_size] = out_buffer.BorrowPutBytes(1);
            CHECK(ok, "Timeout writing filler at end of {}", out_buffer.id());
            auto this_write_len = std::min(remaining_length, write_size);
            out_buffer.FinishPutBytes(this_write_len);
            remaining_length -= this_write_len;
        }
    }

    // Any error messages received while writing the data.
    s3::PutObjectResponseReceivedHandler handler_container_for_crt_bug;
    PushStreambuf out_buffer;
    size_t size;
    size_t part_number;
    std::string etag;
};

}  // namespace s3_plotter

class MultipartUploadFile : public WriteFile {
public:
    MultipartUploadFile() {}

    explicit MultipartUploadFile(
        StorageContext storage_context,
        size_t alignment,
        size_t size_upper_bound)
        : WriteFile(std::move(storage_context), alignment),
          part_size_(s3_plotter::GetPartSizeForAlignment(alignment)),
          content_length_size_(size_upper_bound),
          buffer_size_(buffers::GetClosestAlignedToTarget(
              alignment,
              buffers::kTargetMultipartWriteBufferSize))
    {
    }

    static MultipartUploadFile CreateForOverwrite(FileInfo file_info)
    {
        return MultipartUploadFile(
            std::move(file_info.storage_context), file_info.alignment, file_info.size);
    }

    static MultipartUploadFile CreateCompleted(FileInfo file_info)
    {
        auto file =
            MultipartUploadFile(std::move(file_info.storage_context), file_info.alignment, 0);
        file.write_end_ = file_info.size;
        file.completed_ = true;
        return file;
    }

    MultipartUploadFile(MultipartUploadFile &&other) = default;
    MultipartUploadFile &operator=(MultipartUploadFile &&other) = default;

    void Flush()
    {
        if (current_part_state_) {
            current_part_state_->out_buffer.sync();
        }
    }

    void StartClose()
    {
        if (!closed_) {
            CHECK_LE(write_end_, content_length_size_);
            FinishCurrentPart();
        }
        closed_ = true;
    }

    void AwaitClose()
    {
        if (completed_ || upload_id_.id == "") {
            return;
        }

        if (!closed_) {
            StartClose();
        }

        const auto parts_error_message = WaitForParts();
        if (!parts_error_message.empty()) {
            Abort();
            SPDLOG_FATAL(parts_error_message);
        }

        auto error_message = CompleteMultipartUpload(
            s3_plotter::shared_client_.get(), storage_context_, upload_id_, pending_part_uploads_);
        if (!error_message.empty()) {
            Abort();
            SPDLOG_FATAL(error_message);
        }
        LogSizeStats();
        completed_ = true;
    }

    ~MultipartUploadFile()
    {
        if (completed_ || upload_id_.id == "") {
            return;
        }
        Abort();
    }

    void Write(uint64_t begin, const uint8_t *src, uint64_t length)
    {
        if (length == 0) {
            return;
        }
        DCHECK_GE(begin, write_end_);
        DCHECK(!closed_);

        if (begin > write_end_) {
            WriteArbitrary(begin - write_end_);
            current_part_state_->WriteArbitrary(begin - write_end_);
        }
        WriteImpl(begin, src, length);
    }

    void WriteImpl(uint64_t begin, const uint8_t *src, uint64_t length)
    {
        // Write the actual buffer.
        PrepareForWrite(begin);
        const auto this_write_len = std::min(length, this_part_end() - begin);
        const auto bytes_written = current_part_state_->out_buffer.xsputn(c(src), this_write_len);
        CHECK_EQ(bytes_written, static_cast<ssize_t>(this_write_len));
        write_end_ += this_write_len;
        DCHECK_LE(write_end_, content_length_size_);
        if (this_write_len < length) {
            WriteImpl(begin + this_write_len, src + this_write_len, length - this_write_len);
        }
    }

    void WriteArbitrary(size_t length)
    {
        PrepareForWrite(write_end_);
        const auto this_write_len = std::min(length, this_part_end() - write_end_);
        current_part_state_->WriteArbitrary(this_write_len);
        write_end_ += this_write_len;
        if (this_write_len < length) {
            WriteArbitrary(length - this_write_len);
        }
    }

    void WriteAppend(const uint8_t *src, uint64_t length) { Write(write_end_, src, length); }

    bool is_complete() const { return completed_ && !aborted_; }

    bool aborted() const { return aborted_; }

    void LogSizeStats() const
    {
        SPDLOG_INFO(
            "file {} was {}/{} content_length_size, {:.3f}",
            storage_context_.local_filename,
            write_end_,
            content_length_size_,
            static_cast<double>(write_end_) / content_length_size_);
    }

private:
    size_t part_index() const { return pending_part_uploads_.size(); }
    size_t this_part_end() const
    {
        return std::min((part_index() + 1) * part_size_, content_length_size_);
    }

    const MultipartUploadId &upload_id() const { return upload_id_; }

    void StartUpload()
    {
        auto [error_message, upload_id] =
            CreateMultipartUpload(s3_plotter::shared_client_.get(), storage_context_);
        CHECK_EQ(error_message, "");
        upload_id_ = std::move(upload_id);
    }

    void PrepareForWrite(size_t begin)
    {
        if (likely(current_part_state_ && begin < this_part_end())) {
            // Part is ready.
            return;
        }
        CHECK_LT(begin, content_length_size_);

        if (upload_id_.id.empty()) {
            StartUpload();
        } else {
            // Save the old one.
            write_end_ = FinishCurrentPart();
        }

        StartNextPart();
    }

    size_t FinishCurrentPart();
    void StartNextPart();

    void Abort()
    {
        // Abnormal termination, abort.
        auto abort_error_message =
            AbortMultipartUpload(s3_plotter::shared_client_.get(), storage_context_, upload_id_);
        completed_ = true;
        aborted_ = true;
        if (!abort_error_message.empty()) {
            SPDLOG_ERROR("Failed to abort upload {} {}", upload_id_.id, abort_error_message);
        }
        upload_id_ = {};
    }

    void UploadPartAsync(std::shared_ptr<s3_plotter::UploadPartState> state);
    std::string WaitForParts();

    size_t part_size_ = 0;

    MultipartUploadId upload_id_ = {};
    size_t content_length_size_ = 0;
    size_t buffer_size_ = 0;
    bool aborted_ = false;
    bool completed_ = false;

    std::shared_ptr<s3_plotter::UploadPartState> current_part_state_;
    std::vector<std::shared_ptr<s3_plotter::UploadPartState>> pending_part_uploads_;
};

#endif  // SRC_CPP_MULTIPART_UPLOAD_FILE_HPP_

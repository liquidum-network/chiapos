#ifndef SRC_CPP_S3_SYNC_WRITE_FILE_HPP_
#define SRC_CPP_S3_SYNC_WRITE_FILE_HPP_

#include <array>
#include <cstring>

#include "aws_async.hpp"
#include "buffers.hpp"
#include "disk.hpp"
#include "logging_helpers.hpp"
#include "push_streambuf.hpp"
#include "storage.hpp"
#include "write_file.hpp"

namespace s3_plotter {

struct WriteState : public AsyncState {
    WriteState(std::string key, size_t buffer_size)
        : out_buffer(buffer_size, key.substr(69), 40 * 1000, 12 * 1000, 16 * 1024, 0, 32)
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
};

}  // namespace s3_plotter

class S3SyncWriteFile : public WriteFile {
public:
    S3SyncWriteFile() {}

    explicit S3SyncWriteFile(
        StorageContext storage_context,
        size_t alignment,
        size_t size_upper_bound,
        bool completed = false,
        size_t file_size = 0)
        : WriteFile(std::move(storage_context), alignment, file_size),
          content_length_size_(size_upper_bound),
          completed_(true)
    {
    }

    S3SyncWriteFile(FileInfo file_info)
        : S3SyncWriteFile(
              std::move(file_info.storage_context),
              file_info.alignment,
              file_info.size,
              true,
              file_info.size)
    {
    }

    S3SyncWriteFile(S3SyncWriteFile &&other) = default;
    S3SyncWriteFile &operator=(S3SyncWriteFile &&other) = default;

    S3SyncWriteFile(const S3SyncWriteFile &) = delete;
    S3SyncWriteFile &operator=(const S3SyncWriteFile &) = delete;

    void Flush()
    {
        if (write_state_) {
            write_state_->out_buffer.sync();
        }
    }

    void StartClose()
    {
        if (write_state_ && !closed_) {
            // Signal to the write thread that we're done.
            CHECK_LE(write_end_, content_length_size_);

            // S3 requires a content length for PutObject, so we have to
            // overestimate the size of the file and write out random shit to
            // fill in at the end.
            write_state_->out_buffer.SetWriteDoneWithPadding(content_length_size_ - write_end_);
        }
        closed_ = true;
    }

    void AwaitClose()
    {
        if (!closed_) {
            StartClose();
        }
        DCHECK(closed_);
        if (write_state_) {
            WaitForWrite(write_state_.get());
            SPDLOG_TRACE("{} closed after writing {} bytes", key(), write_end_);
            write_state_ = nullptr;
        }
        completed_ = true;
    }

    // TODO: Add abort/automatic cleanup.

    ~S3SyncWriteFile() { AwaitClose(); }

    void Write(uint64_t begin, const uint8_t *src, uint64_t length)
    {
        if (length == 0) {
            return;
        }
        DCHECK_GE(begin, write_end_);
        DCHECK(!closed_);

        // TODO: This check sucks, don't do it.
        EnsureWriteStarted();

        if (begin > write_end_) {
            write_state_->WriteArbitrary(begin - write_end_);
            write_end_ = begin;
        }

        // Write the actual buffer.
        const auto bytes_written = write_state_->out_buffer.xsputn(c(src), length);
        CHECK_EQ(bytes_written, static_cast<ssize_t>(length), key());
        write_end_ += length;
        CHECK_LE(write_end_, content_length_size_);
    }

    void WriteAppend(const uint8_t *src, uint64_t length) { Write(write_end_, src, length); }
    void WriteAppendAligned(const uint8_t *src) { Write(write_end_, src, alignment_); }

    void WriteArbitrary(size_t length)
    {
        if (length == 0) {
            return;
        }
        EnsureWriteStarted();
        write_state_->WriteArbitrary(length);
    }

    void LogSizeStats() const
    {
        SPDLOG_DEBUG(
            "file {} was {}/{} content_length_size, {:.3f}",
            storage_context_.local_filename,
            write_end_,
            content_length_size_,
            static_cast<double>(write_end_) / content_length_size_);
    }

    bool is_complete() const { return completed_; }

private:
    void CheckErrors()
    {
        auto &state = *write_state_;
        if (state.done()) {
            return;
        }

        const std::scoped_lock lock(state.mutex);
        CHECK_EQ(state.error_message, "");
    }

    void EnsureWriteStarted()
    {
        if (write_state_) {
            return;
        }
        write_state_ = StartWrite();
    }

    void WaitForWrite(s3_plotter::WriteState *state)
    {
        SPDLOG_TRACE("{} await closing write", key());
        if (!state->AwaitDone()) {
            // Undo the pending_read flag so that the buffer is later freed.
            SPDLOG_FATAL("Timed out waiting for write");
        }

        // TODO: Propagate errors.
        CHECK_EQ(state->error_message, "");
    }

    std::shared_ptr<s3_plotter::WriteState> StartWrite();

    std::shared_ptr<s3_plotter::WriteState> write_state_;
    size_t content_length_size_ = 0;
    bool completed_ = false;
};
using StreamingWriteFile = S3SyncWriteFile;

#endif  // SRC_CPP_S3_SYNC_WRITE_FILE_HPP_

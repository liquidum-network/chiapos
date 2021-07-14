#ifndef SRC_CPP_S3_SYNC_READ_FILE_HPP_
#define SRC_CPP_S3_SYNC_READ_FILE_HPP_

#include <cstring>
#include <utility>

#include "aws_async.hpp"
#include "buffers.hpp"
#include "disk.hpp"
#include "locking_concurrent_streambuf.hpp"
#include "logging_helpers.hpp"
#include "nlohmann/json.hpp"
#include "push_streambuf.hpp"
#include "s3_plotter.hpp"
#include "storage.hpp"
#include "types.hpp"

namespace s3_plotter {

struct ReadState : public AsyncState {
    ReadState(std::string key, size_t buffer_size)
        : key(std::move(key)),
          in_buffer(buffer_size, this->key.substr(69), 30 * 1000, 60 * 1000, 0, 16 * 1024, 0)
    {
    }

    // Any error messages received while writing the data.
    s3::GetObjectResponseReceivedHandler handler_container_for_crt_bug;
    std::string key;
    PushStreambuf in_buffer;
};

}  // namespace s3_plotter

struct S3SyncReadFile : public ReadDisk {
    S3SyncReadFile() {}

    explicit S3SyncReadFile(StorageContext storage_context, size_t file_size, size_t alignment)
        : storage_context_(std::move(storage_context)), file_size_(file_size), alignment_(alignment)
    {
    }

    S3SyncReadFile(FileInfo file_info)
        : S3SyncReadFile(std::move(file_info.storage_context), file_info.size, file_info.alignment)
    {
    }

    S3SyncReadFile(S3SyncReadFile &&other) = default;
    S3SyncReadFile &operator=(S3SyncReadFile &&other) = default;
    ~S3SyncReadFile() override final { Close(); }

    const uint8_t *BorrowAlignedAt(size_t begin, size_t length) override final
    {
        // This check sucks, don't do it.
        EnsureReadStarted();

        size_t borrowed_read_size = 0;
        const uint8_t *read_ptr = nullptr;
        while (read_ptr == nullptr || begin >= read_pos_ + borrowed_read_size) {
            read_state_->in_buffer.FinishGetBytes(borrowed_read_size);
            read_pos_ += borrowed_read_size;
            auto [ok, this_read_ptr, this_read_size] =
                read_state_->in_buffer.BorrowGetBytes(length);
            CHECK(ok, "timeout reading bytes from {}", key());

            read_ptr = this_read_ptr;
            borrowed_read_size = this_read_size;
        }

        DCHECK_GE(begin, read_pos_);
        DCHECK_LE(begin + length, read_pos_ + borrowed_read_size);

        return read_ptr + begin - read_pos_;
    }

    const uint8_t *BorrowOneAligned() override final
    {
        // This check sucks, don't do it.
        EnsureReadStarted();

        // TODO: pass errors up
        SPDLOG_TRACE("{} bytes reading", key());
        read_state_->in_buffer.FinishGetBytes(borrowed_read_size_);
        read_pos_ += borrowed_read_size_;
        auto [ok, read_ptr, size] = read_state_->in_buffer.BorrowGetBytes(alignment_);
        CHECK(ok, "timeout reading bytes from {}", key());
        DCHECK_GE(size, alignment_);
        SPDLOG_TRACE("{} bytes done reading {}", key(), size);
        borrowed_read_size_ = alignment_;
        return read_ptr;
    }

    std::pair<uint8_t *, size_t> BorrowAligned()
    {
        // This check sucks, don't do it.
        EnsureReadStarted();

        // TODO: pass errors up
        SPDLOG_TRACE("{} bytes reading", key());
        read_state_->in_buffer.FinishGetBytes(borrowed_read_size_);
        read_pos_ += borrowed_read_size_;
        auto [ok, read_ptr, size] = read_state_->in_buffer.BorrowGetBytes(alignment_);
        CHECK(ok, "timeout reading bytes from {}", key());
        CHECK_GE(size, alignment_);
        SPDLOG_TRACE("{} bytes done reading {}", key(), size);
        borrowed_read_size_ = (size / alignment_) * alignment_;
        return {read_ptr, borrowed_read_size_};
    }

    std::string ReadEntireFile(
        uint8_t *dest,
        size_t read_size,
        uint64_t timeout_millis = 1000 * 60 * 10);

    void Close() override final
    {
        // TODO: Maybe cancel the read?
        if (read_state_) {
            read_state_->in_buffer.SetReadDone();
            read_state_ = nullptr;
        }
        closed_ = true;
    }

    void FreeMemory() override final
    {
        // TODO: Implement.
        Close();
    }

    void CloseAndDelete() override final;

    const std::string &bucket() const { return storage_context_.bucket; }
    std::string key() const { return storage_context_.GetS3Key(); }
    std::string GetFileName() const { return storage_context_.GetUri(); }
    size_t GetFileSize() const { return file_size_; }
    const StorageContext &GetStorageContext() const { return storage_context_; }

private:
    static const char *c(const uint8_t *ptr) { return reinterpret_cast<const char *>(ptr); }

    void CheckErrors()
    {
        auto &state = *read_state_;
        if (state.done()) {
            return;
        }

        const std::scoped_lock lock(state.mutex);
        CHECK_EQ(state.error_message, "");
    }

    void EnsureReadStarted()
    {
        DCHECK(!closed_);
        // TODO: Unlike writes, reads can be restarted.
        // Restart if there is an error.
        if (read_state_) {
            return;
        }
        read_state_ = StartRead();
    }

    std::shared_ptr<s3_plotter::ReadState> StartRead();

    StorageContext storage_context_;
    size_t file_size_ = 0;
    size_t alignment_ = 0;
    std::shared_ptr<s3_plotter::ReadState> read_state_;
    size_t borrowed_read_size_ = 0;
    size_t read_pos_ = 0;
    bool closed_ = false;
};
using StreamingReadFile = S3SyncReadFile;

#endif  // SRC_CPP_S3_SYNC_READ_FILE_HPP_

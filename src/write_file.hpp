#ifndef SRC_CPP_WRITE_FILE_HPP_
#define SRC_CPP_WRITE_FILE_HPP_

#include "disk.hpp"
#include "logging.hpp"
#include "s3_plotter.hpp"
#include "storage.hpp"

class WriteFile {
public:
    WriteFile(FileInfo file_info)
        : WriteFile(std::move(file_info.storage_context), file_info.alignment, file_info.size)
    {
    }

    FileInfo GetFileInfo() const
    {
        return FileInfo{
            .storage_context = storage_context_, .size = write_end_, .alignment = alignment_};
    }

    WriteFile(const WriteFile &) = delete;
    WriteFile &operator=(const WriteFile &) = delete;
    WriteFile(WriteFile &&) = default;
    WriteFile &operator=(WriteFile &&) = default;

    void Truncate(uint64_t new_size)
    {
        DCHECK(!closed_);
        CHECK_LE(new_size, write_end_);
        write_end_ = new_size;
    }

    const StorageContext &GetStorageContext() const { return storage_context_; }
    std::string GetUri() const { return storage_context_.GetUri(); }
    const std::string &bucket() const { return storage_context_.bucket; }
    std::string key() const { return storage_context_.GetS3Key(); }
    std::string GetFileName() const { return GetUri(); }

    size_t GetFileSize() const { return write_end_; }

protected:
    WriteFile() {}

    WriteFile(StorageContext storage_context, size_t alignment, size_t write_end = 0)
        : storage_context_(std::move(storage_context)), alignment_(alignment), write_end_(write_end)
    {
        s3_plotter::InitS3Client();
    }

    static const char *c(const uint8_t *ptr) { return reinterpret_cast<const char *>(ptr); }

    StorageContext storage_context_;
    size_t alignment_;
    size_t write_end_ = 0;
    bool closed_ = false;
};

#endif  // SRC_CPP_WRITE_FILE_HPP_

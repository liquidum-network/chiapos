#ifndef SRC_CPP_FILE_RAW_HPP_
#define SRC_CPP_FILE_RAW_HPP_

#include <fcntl.h>
#include <fmt/core.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>

#include "logging.hpp"
#include "logging_helpers.hpp"
#include "storage.hpp"

class FileRaw {
public:
    static FileRaw Create(const std::filesystem::path &filename, bool delete_before_open)
    {
        return FileRaw(StorageContext{.local_filename = filename}, delete_before_open);
    }

    static FileRaw CreateWithK(
        StorageContext storage_context,
        uint8_t k_unused,
        bool delete_before_open,
        bool file_size_known = false,
        size_t file_size = 0)
    {
        return FileRaw(std::move(storage_context), delete_before_open);
    }

    static FileRaw CreateForTesting(
        const std::filesystem::path &filename,
        size_t cache_size_unused = 0)
    {
        return FileRaw(StorageContext{.local_filename = filename}, true);
    }

    void Open()
    {
        // if the file is already open, don't do anything
        if (fileno_ > 0)
            return;
        fileno_ = open(GetFileName().c_str(), O_RDWR | O_CREAT, 0600);
        if (fileno_ < 0) {
            std::string error_message =
                "Could not open " + GetFileName() + ": " + ::strerror(errno) + ".";
            throw std::runtime_error(error_message);
        }
    }

    FileRaw(FileRaw &&other)
    {
        storage_context_ = std::move(other.storage_context_);
        fileno_ = other.fileno_;
        other.fileno_ = -1;
    }

    FileRaw(const FileRaw &) = delete;
    FileRaw &operator=(const FileRaw &) = delete;

    void Close()
    {
        if (fileno_ < 0)
            return;
        close(fileno_);
        fileno_ = -1;
    }

    void CloseAndDelete()
    {
        Close();
        std::filesystem::remove(GetFileName());
    }

    std::error_code CloseAndRename(const std::filesystem::path &to)
    {
        std::error_code ec;
        Close();
        std::filesystem::rename(GetFileName(), to, ec);
        return ec;
    }

    ~FileRaw() { Close(); }

    void Read(uint64_t begin, uint8_t *memcache, uint64_t length)
    {
        Open();
        logging::LogDiskAccess(GetFileName(), "read", begin, length);
        const auto result = pread(fileno_, memcache, length, begin);
        if (result < 0) {
            throw std::runtime_error(std::string("bad read: ") + ::strerror(errno));
        }
        if (static_cast<uint64_t>(result) != length) {
            throw std::runtime_error(fmt::format(
                "read too few bytes: requested {}+{} from {} file {}",
                begin,
                length,
                FileSize(),
                GetFileName()));
        }
    }

    void Write(uint64_t begin, const uint8_t *memcache, uint64_t length)
    {
        Open();
        logging::LogDiskAccess(GetFileName(), "write", begin, length);
        const auto result = pwrite(fileno_, memcache, length, begin);
        if (result < 0) {
            throw std::runtime_error(std::string("bad write: ") + ::strerror(errno));
        }
        if (static_cast<uint64_t>(result) != length) {
            throw std::runtime_error("wrote too few bytes");
        }
    }

    std::string GetFileName() const { return storage_context_.FullLocalPath(); }
    const StorageContext &GetStorageContext() const { return storage_context_; }

    void Truncate(uint64_t new_size)
    {
        Close();
        logging::LogDiskAccess(GetFileName(), "truncate", 0, 0);
        const auto result = truncate(GetFileName().c_str(), new_size);
        if (result < 0) {
            throw std::runtime_error("bad truncate");
        }
    }

private:
    explicit FileRaw(StorageContext storage_context, bool delete_before_open)
        : storage_context_(std::move(storage_context)), fileno_(-1)
    {
        if (delete_before_open) {
            std::filesystem::remove(GetFileName());
        }
        Open();
    }

    size_t FileSize() const
    {
        struct stat st;
        logging::LogDiskAccess(GetFileName(), "stat", 0, 0);
        const auto result = fstat(fileno_, &st);
        if (result < 0) {
            throw std::runtime_error(std::string("couldn't get size: ") + ::strerror(errno));
        }

        return st.st_size;
    }

    StorageContext storage_context_;
    int fileno_;
};

#endif  // SRC_CPP_FILE_RAW_HPP_

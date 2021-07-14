#ifndef SRC_CPP_FINAL_FILE_HPP_
#define SRC_CPP_FINAL_FILE_HPP_

#include <string>
#include <utility>

#include "disk.hpp"
#include "logging.hpp"
#include "multipart_upload_file.hpp"
#include "nlohmann/json.hpp"
#include "s3_sync_write_file.hpp"
#include "storage.hpp"
#include "types.hpp"

class FinalFile {
public:
    FinalFile() : k_(0), plot_id_hex_("") {}
    FinalFile(uint8_t k, std::string plot_id_hex) : k_(k), plot_id_hex_(std::move(plot_id_hex)) {}

    void OpenTable(Tables table, size_t alignment, size_t size_upper_bound)
    {
        auto table_index = TableToIndex(table);
        if (table_index <= 7) {
            CHECK_EQ(table_index, t_files_.size() + 1, "Tables must be opened in order");
            t_files_.emplace_back(
                MultipartUploadFile(GetStorageContextForTable(table), alignment, size_upper_bound));
        } else {
            CHECK_EQ(table_index, c_files_.size() + 8, "Tables must be opened in order");
            c_files_.emplace_back(
                StreamingWriteFile(GetStorageContextForTable(table), alignment, size_upper_bound));
        }
    }

    void CheckComplete() const
    {
        for (size_t i = 0; i < t_files_.size(); ++i) {
            CHECK(t_files_[i].is_complete(), "Table {} not complete", i + 1);
        }
        for (size_t i = 0; i < c_files_.size(); ++i) {
            CHECK(c_files_[i].is_complete(), "Table {} not complete", i + 8);
        }
    }

    void StartCloseTable(Tables table)
    {
        const auto table_index = TableToIndex(table);
        if (table_index <= 7) {
            GetTFile(table_index).StartClose();
        } else {
            GetCFile(table_index).StartClose();
        }
    }

    void AwaitCloseTable(Tables table)
    {
        const auto table_index = TableToIndex(table);
        if (table_index <= 7) {
            GetTFile(table_index).AwaitClose();
        } else {
            GetCFile(table_index).AwaitClose();
        }
    }

    void Write(Tables table, size_t begin, const uint8_t *src, uint64_t length)
    {
        const auto table_index = TableToIndex(table);
        if (table_index <= 7) {
            GetTFile(table_index).Write(begin, src, length);
        } else {
            GetCFile(table_index).Write(begin, src, length);
        }
    }

    void WriteArbitrary(Tables table, uint64_t length)
    {
        const auto table_index = TableToIndex(table);
        if (table_index <= 7) {
            GetTFile(table_index).WriteArbitrary(length);
        } else {
            GetCFile(table_index).WriteArbitrary(length);
        }
    }

    size_t GetTableSize(Tables table) const
    {
        const auto table_index = TableToIndex(table);
        if (table_index <= 7) {
            return GetTFile(table_index).GetFileSize();
        } else {
            return GetCFile(table_index).GetFileSize();
        }
    }

    std::string GetTableUri(Tables table) const
    {
        return GetStorageContextForTable(table).GetUri();
    }

    std::vector<FileInfo> GetFileInfos() const
    {
        std::vector<FileInfo> file_infos;
        for (const auto &file : t_files_) {
            file_infos.emplace_back(file.GetFileInfo());
        }
        for (const auto &file : c_files_) {
            file_infos.emplace_back(file.GetFileInfo());
        }
        return file_infos;
    }

private:
    friend void to_json(json &j, const FinalFile &p);
    friend void from_json(const json &j, FinalFile &p);

    FinalFile(uint8_t k, std::string plot_id_hex, std::vector<FileInfo> file_infos)
        : FinalFile(k, std::move(plot_id_hex))
    {
        t_files_.reserve(file_infos.size());
        for (size_t t_i = 0; t_i < std::min(file_infos.size(), 7UL); ++t_i) {
            t_files_.emplace_back(MultipartUploadFile::CreateCompleted(std::move(file_infos[t_i])));
        }
        c_files_.reserve(file_infos.size());
        for (size_t c_i = 7; c_i < std::min(file_infos.size(), 10UL); ++c_i) {
            c_files_.emplace_back(S3SyncWriteFile(std::move(file_infos[c_i])));
        }
    }

    MultipartUploadFile &GetTFile(size_t table_index) { return t_files_[table_index - 1]; }

    const MultipartUploadFile &GetTFile(size_t table_index) const
    {
        return t_files_[table_index - 1];
    }

    StreamingWriteFile &GetCFile(size_t table_index) { return c_files_[table_index - 8]; }

    const StreamingWriteFile &GetCFile(size_t table_index) const
    {
        return c_files_[table_index - 8];
    }

    // StorageContext GetStorageContextForTable(Tables table) const
    //{
    //// TODO: Copy parts to final location with get-range.
    // return StorageContext::CreateTemp(
    // k_, plot_id_hex_, fmt::format("final_table_{}.dat", TableToIndex(table)));
    //}

    StorageContext GetStorageContextForTable(Tables table) const
    {
        auto context = StorageContext::CreateForFinalPlot(k_, table, plot_id_hex_);
        const auto table_index = TableToIndex(table);
        if (k_ >= 32 && (1 <= table_index || table_index <= 7 || table_index == 10)) {
            context = context.WithColdStorageClass();
        } else {
            context = context.WithWarmStorageClass();
        }
        return context;
    }

    uint8_t k_;
    std::string plot_id_hex_;
    std::vector<MultipartUploadFile> t_files_;
    std::vector<StreamingWriteFile> c_files_;
};

using json = nlohmann::json;
inline void to_json(json &j, const FinalFile &p)
{
    j = json{
        {"k", p.k_},
        {"plot_id_hex", p.plot_id_hex_},
        {"files", p.GetFileInfos()},
    };
}

inline void from_json(const json &j, FinalFile &p)
{
    p = FinalFile(j.at("k"), j.at("plot_id_hex"), j.at("files"));
}

#endif  // SRC_CPP_FINAL_FILE_HPP_

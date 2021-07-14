#pragma once

#include "bitfield.hpp"
#include "nlohmann/json.hpp"
#include "s3_delete.hpp"
#include "s3_read.hpp"
#include "s3_utils.hpp"
#include "storage.hpp"

class BitfieldS3Backed {
public:
    BitfieldS3Backed() {}
    BitfieldS3Backed(StorageContext storage_context, size_t size_bytes)
        : storage_context_(std::move(storage_context)),
          bitfield_(8 * size_bytes),
          needs_load_(size_bytes > 0),
          needs_save_(false)
    {
    }

    BitfieldS3Backed(StorageContext storage_context, Bitfield bitfield)
        : storage_context_(std::move(storage_context)),
          bitfield_(std::move(bitfield)),
          needs_save_(true)
    {
        DCHECK_GT(bitfield_.size(), 0);
    }

    BitfieldS3Backed(BitfieldS3Backed &&other)
    {
        // TODO: This can probably be optimized.
        std::scoped_lock lock(other.save_lock_);
        storage_context_ = std::move(other.storage_context_);
        bitfield_ = std::move(other.bitfield_);
        needs_load_ = other.needs_load_;
        needs_save_ = other.needs_save_;
        other.needs_load_ = false;
        other.needs_save_ = false;
    }

    BitfieldS3Backed &operator=(BitfieldS3Backed &&other)
    {
        // TODO: This can probably be optimized.
        std::scoped_lock lock(other.save_lock_, save_lock_);
        storage_context_ = std::move(other.storage_context_);
        bitfield_ = std::move(other.bitfield_);
        needs_load_ = other.needs_load_;
        needs_save_ = other.needs_save_;
        other.needs_load_ = false;
        other.needs_save_ = false;
        return *this;
    }

    Bitfield &&StealBitfield()
    {
        EnsureLoaded();
        return std::move(bitfield_);
    }

    void EnsureSaved() const
    {
        if (!needs_save_) {
            return;
        }
        std::scoped_lock lock(save_lock_);
        if (!needs_save_) {
            return;
        }

        auto error_message = WriteBufferToS3(
            s3_plotter::shared_client_.get(),
            storage_context_.bucket,
            storage_context_.GetS3Key(),
            bitfield_.data(),
            bitfield_.size_bytes());
        CHECK_EQ(error_message, "");

        // Save to S3.
        needs_save_ = false;
    }

    void Delete()
    {
        if (size_bytes() == 0) {
            // DCHECK_EQ(storage_context_.bucket, "");
            return;
        }
        RemoveS3ObjectIgnoreResult(
            s3_plotter::shared_client_.get(), storage_context_.bucket, storage_context_.GetS3Key());
    }

    void EnsureLoaded()
    {
        if (!needs_load_) {
            return;
        }
        const auto [error_message, bytes_read] = ReadBufferFromS3(
            s3_plotter::shared_client_.get(),
            storage_context_.bucket,
            storage_context_.GetS3Key(),
            bitfield_.data(),
            -1,
            bitfield_.size_bytes());
        CHECK_EQ(error_message, "");
        CHECK_EQ(bytes_read, bitfield_.size_bytes());
        needs_load_ = false;
    }

    size_t size_bytes() const { return bitfield_.size_bytes(); }
    Bitfield &bitfield_no_check_loaded() { return bitfield_; }
    bool needs_save() const { return needs_save_; }

    const Bitfield &bitfield()
    {
        EnsureLoaded();
        return bitfield_;
    }

private:
    friend void to_json(json &j, const BitfieldS3Backed &p);
    friend void from_json(const json &j, BitfieldS3Backed &p);

    mutable std::mutex save_lock_;
    StorageContext storage_context_;
    Bitfield bitfield_;
    bool needs_load_ = false;
    mutable bool needs_save_ = false;
};

inline void to_json(json &j, const BitfieldS3Backed &p)
{
    if (p.size_bytes() == 0) {
        j == nullptr;
        return;
    }
    p.EnsureSaved();

    j = json{
        {"storage_context", p.storage_context_},
        {"size_bytes", p.size_bytes()},
    };
}

inline void from_json(const json &j, BitfieldS3Backed &p)
{
    if (j == nullptr) {
        p = {};
        return;
    }
    p = BitfieldS3Backed(j.at("storage_context"), j.at("size_bytes"));
}

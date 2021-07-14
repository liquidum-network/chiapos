#ifndef SRC_CPP_STORAGE_HPP_
#define SRC_CPP_STORAGE_HPP_

#include <filesystem>
#include <string>

#include "aws_globals.hpp"
#include "fmt/core.h"
#include "nlohmann/json.hpp"
#include "types.hpp"

using json = nlohmann::json;

constexpr auto kMockTmpDir = "/tmp/mock-s3/";

struct StorageContext {
    static StorageContext CreateTemp(
        uint8_t k,
        const std::string& plot_id_hex,
        const std::string& filename)
    {
        auto key_prefix = fmt::format("{}-k{}", plot_id_hex, k);

        return StorageContext{
            .bucket = "plot-tmp-" + GetAwsRegion(),
            .key_prefix = key_prefix,
            .local_filename = filename,
        };
    }

    static StorageContext CreateForFinalPlot(
        uint8_t k,
        Tables table,
        const std::string& plot_id_hex)
    {
        auto bucket = "chiamine-" + GetAwsRegion();
        auto key_prefix = fmt::format(
            "{}plots2/{:02}/{}",
            k < 32 ? "test-" : "",
            TableToIndex(table),
            plot_id_hex.substr(0, 8));

        return StorageContext{
            .bucket = std::move(bucket),
            .key_prefix = std::move(key_prefix),
            .local_filename = plot_id_hex.substr(8),
        };
    }

    static StorageContext CreateForManifest(uint8_t k, const std::string& plot_id_hex)
    {
        auto bucket = "chiamine-" + GetAwsRegion();
        auto key_prefix = std::string{k < 32 ? "test-" : ""} + "plot-manifests";

        return StorageContext{
            .bucket = std::move(bucket),
            .key_prefix = std::move(key_prefix),
            .local_filename = plot_id_hex + ".json",
        };
    }

    StorageContext WithSuffix(std::string suffix) const
    {
        auto copy = *this;
        copy.local_filename += std::move(suffix);
        return copy;
    }

    StorageContext WithBucket(std::string bucket) const
    {
        auto copy = *this;
        copy.bucket = std::move(bucket);
        return copy;
    }

    StorageContext WithColdStorageClass() const
    {
        auto copy = *this;
        copy.use_cold_storage_class = true;
        return copy;
    }

    StorageContext WithWarmStorageClass() const
    {
        auto copy = *this;
        copy.use_cold_storage_class = false;
        return copy;
    }

    std::string GetUri() const
    {
#if USE_MOCK_S3 == 1
        return std::string{"file://"} + kMockTmpDir + bucket + "/" + GetS3Key();
#else
        return "s3://" + bucket + "/" + GetS3Key();
#endif
    }

    std::string GetS3Key() const { return key_prefix + "/" + local_filename.string(); }

    std::string bucket;
    std::string key_prefix;
    std::filesystem::path local_filename;
    bool use_cold_storage_class = false;
};

inline void to_json(json& j, const StorageContext& p)
{
    j = json{
        {"bucket", p.bucket},
        {"key_prefix", p.key_prefix},
    };
    if (!p.local_filename.empty()) {
        j["local_filename"] = p.local_filename;
    }
    if (p.use_cold_storage_class) {
        j["use_cold_storage_class"] = true;
    }
}

inline void from_json(const json& j, StorageContext& p)
{
    p.bucket = j.value("bucket", "");
    p.key_prefix = j.value("key_prefix", "");
    p.local_filename = j.value("local_filename", "");
    p.use_cold_storage_class = j.value("use_cold_storage_class", false);
}

#endif  // SRC_CPP_STORAGE_HPP_

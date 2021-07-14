#include "manifest.hpp"

#include "final_file.hpp"
#include "logging.hpp"
#include "nlohmann/json.hpp"
#include "s3_utils.hpp"
#include "serialization.hpp"
#include "types.hpp"

namespace {
using json = nlohmann::json;
}

std::string CreatePlotManifest(
    const FinalFile& final_file,
    uint8_t k,
    const std::vector<uint8_t>& id,
    const std::vector<uint8_t>& memo,
    const std::vector<size_t>& table_offsets)
{
    json manifest_json;
    manifest_json["v"] = "v1.0";
    manifest_json["vv"] = "1.0";
    manifest_json["id"] = BytesToHex(id);
    manifest_json["k"] = k;
    manifest_json["m"] = BytesToHex(memo);

    json table_ptrs = json::array();
    for (size_t table_idx = 1; table_idx < 11; ++table_idx) {
        json table_ptr;
        const auto table = IndexToTable(table_idx);
        if (table == Tables::C3) {
            // Table C3 can be a bit smaller
            CHECK_LE(
                final_file.GetTableSize(table),
                table_offsets[table_idx + 1] - table_offsets[table_idx],
                "table_idx {}",
                table_idx);

            CHECK_GT(
                8 * final_file.GetTableSize(table),
                table_offsets[table_idx + 1] - table_offsets[table_idx],
                "table_idx {}",
                table_idx);
        } else {
            CHECK_EQ(
                final_file.GetTableSize(table),
                table_offsets[table_idx + 1] - table_offsets[table_idx],
                "table_idx {}",
                table_idx);
        }
        table_ptr["uri"] = final_file.GetTableUri(table);
        table_ptrs.emplace_back(std::move(table_ptr));
    }

    // Check table sizes.

    manifest_json["sz"] = table_offsets[11];
    manifest_json["ptr"] = table_ptrs;

    return manifest_json.dump();
}

std::pair<std::string, StorageContext> UploadManifestToS3(
    StorageContext manifest_storage,
    const std::string& manifest_bytes)
{
    auto error_message = WriteBufferToS3(
        s3_plotter::shared_client_.get(),
        manifest_storage.bucket,
        manifest_storage.GetS3Key(),
        reinterpret_cast<const uint8_t*>(manifest_bytes.data()),
        manifest_bytes.size());

    return {error_message, manifest_storage};
}

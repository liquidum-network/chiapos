#ifndef SRC_CPP_MANIFEST_HPP_
#define SRC_CPP_MANIFEST_HPP_

#include <string>
#include <utility>
#include <vector>

#include "final_file.hpp"

std::string CreatePlotManifest(
    const FinalFile& final_file,
    uint8_t k,
    const std::vector<uint8_t>& id,
    const std::vector<uint8_t>& memo,
    const std::vector<size_t>& table_offsets);

std::pair<std::string, StorageContext> UploadManifestToS3(
    StorageContext manifest_storage,
    const std::string& manifest_bytes);

#endif  // SRC_CPP_MANIFEST_HPP_


#ifndef SRC_CPP_PLOT_FILE_CACHE_HPP_
#define SRC_CPP_PLOT_FILE_CACHE_HPP_

#include <filesystem>
#include <string>

#include "s3_utils.hpp"

// Returns a file number pointing to a local cache Load the cloud storage object from the specified
// URI from
//
int LoadCachedFileForS3URI(
    S3Client* client,
    const std::filesystem::path& cache_base_dir,
    const std::string& uri);

#endif  // SRC_CPP_PLOT_FILE_CACHE_HPP_

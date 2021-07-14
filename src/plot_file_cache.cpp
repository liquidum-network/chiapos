
#include "plot_file_cache.hpp"

#include <fcntl.h>
#include <fmt/core.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>

#include "s3_utils.hpp"

#if USE_MOCK_S3 != 1

#include "aws/core/utils/FileSystemUtils.h"

int LoadCachedFileForS3URI(
    S3Client* client,
    const std::filesystem::path& cache_base_dir,
    const std::string& uri)
{
    const auto [bucket, remote_path] = parse_s3_uri(uri);

    const auto cache_base_dir_path = std::filesystem::path(cache_base_dir);
    const auto local_path = cache_base_dir_path / remote_path;

    if (!std::filesystem::exists(local_path)) {
        const auto temp_download_path = cache_base_dir_path / "working" / remote_path;
        {
            std::error_code ec;
            std::filesystem::create_directories(temp_download_path.parent_path(), ec);
            if (ec) {
                throw std::runtime_error(fmt::format(
                    "Could not create path {}: {}",
                    temp_download_path.parent_path().string(),
                    ec.message()));
            }
        }
        {
            std::error_code ec;
            std::filesystem::create_directories(local_path.parent_path(), ec);
            if (ec) {
                throw std::runtime_error(fmt::format(
                    "Could not create path {}: {}",
                    local_path.parent_path().string(),
                    ec.message()));
            }
        }

        {
            // Download to local cache.
            // Locally scoped to flush the write before rename.
            auto request = s3::Model::GetObjectRequest().WithBucket(bucket).WithKey(remote_path);
            request.SetResponseStreamFactory([temp_download_path]() {
                return Aws::New<Aws::FStream>(
                    "",
                    temp_download_path.c_str(),
                    std::ios::binary | std::ios::out | std::ios_base::trunc);
            });
            const auto outcome = client->GetObject(request);
            if (!outcome.IsSuccess()) {
                throw std::runtime_error(fmt::format(
                    "failed to read s3://{}/{} : {}",
                    bucket,
                    remote_path,
                    outcome.GetError().GetMessage()));
            }
        }

        // Move into place atomically.
        std::filesystem::rename(temp_download_path, local_path);
    }

    const auto fileno = open(local_path.c_str(), O_RDONLY);
    CHECK_GE(fileno, 0);
    return fileno;
}

#else  // USE_MOCK_S3 == 1

int LoadCachedFileForS3URI(
    S3Client* client,
    const std::filesystem::path& cache_base_dir,
    const std::string& uri)
{
    CHECK(is_file_uri(uri));
    auto path = parse_file_uri(uri);
    const auto fileno = open(path.c_str(), O_RDONLY);
    CHECK_GE(fileno, 0);
    return fileno;
}

#endif  // USE_MOCK_S3 == 1

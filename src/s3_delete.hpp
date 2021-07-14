#pragma once

#include <string>

#include "s3_utils.hpp"

void RemoveS3ObjectIgnoreResult(
    S3Client* s3_client_unused,
    const std::string& bucket,
    const std::string& key);

void RemoveS3ObjectTree(S3Client* s3_client, const std::string& bucket, const std::string& prefix);

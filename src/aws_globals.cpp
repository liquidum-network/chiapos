#include "aws_globals.hpp"

#if USE_MOCK_S3 == 1

namespace Aws {
namespace Config {
struct MockProfile {
    std::string GetRegion() const { return "mock-region"; }
};
MockProfile GetCachedConfigProfile(const std::string& name_unused) { return MockProfile{}; }
}  // namespace Config
}  // namespace Aws

#else
#include "aws/core/config/AWSProfileConfigLoader.h"
#endif  // USE_MOCK_S3 == 1

std::string GetAwsRegion() { return Aws::Config::GetCachedConfigProfile("default").GetRegion(); }

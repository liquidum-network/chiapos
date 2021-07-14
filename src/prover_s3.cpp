#include "prover_s3.hpp"

#include <memory>

#if USE_MOCK_S3 != 1

#include "aws/common/common.h"
#include "aws/common/logging.h"
#include "aws/core/Aws.h"
#include "aws/core/client/DefaultRetryStrategy.h"
#include "aws/core/http/Scheme.h"
#include "aws/core/utils/logging/AWSLogging.h"
#include "aws/core/utils/logging/DefaultLogSystem.h"
#include "aws/core/utils/memory/AWSMemory.h"
#include "aws_globals.hpp"
#include "logging.hpp"
#include "s3_executor.hpp"

#if USE_AWS_CRT == 1
#include "aws/s3-crt/ClientConfiguration.h"
#else
#include "aws/core/client/ClientConfiguration.h"
#endif

namespace {

static std::once_flag aws_sdk_init_once_;

static Aws::SDKOptions GetProverAWSSDKOptions()
{
    Aws::SDKOptions options;
    const auto log_level = Aws::Utils::Logging::LogLevel::Warn;
    const std::string log_file_prefix = "prover_s3_aws_sdk_";
    options.loggingOptions.logLevel = log_level;
    options.loggingOptions.logger_create_fn = [log_level,
                                               log_file_prefix = std::move(log_file_prefix)]() {
        const auto user_cstr = getenv("USER");
        const std::filesystem::path logdir =
            fmt::format("/home/{}/.chia/mainnet/log/", user_cstr ? user_cstr : "");
        if (std::filesystem::exists(logdir)) {
            // aws_logger_set_log_level(aws_logger_get(), AWS_LL_TRACE);
            return std::make_shared<Aws::Utils::Logging::DefaultLogSystem>(
                log_level, logdir / log_file_prefix);
        }
        return std::make_shared<Aws::Utils::Logging::DefaultLogSystem>(log_level, log_file_prefix);
    };

#if USE_AWS_CRT == 1
    options.loggingOptions.crt_logger_create_fn = [log_level]() {
        // aws_logger_set_log_level(aws_logger_get(), AWS_LL_TRACE);
        return std::make_shared<Aws::Utils::Logging::DefaultCRTLogSystem>(log_level);
    };
    options.ioOptions.clientBootstrap_create_fn = []() {
        Aws::Crt::Io::EventLoopGroup eventLoopGroup(0, 200);
        Aws::Crt::Io::DefaultHostResolver defaultHostResolver(
            eventLoopGroup, 8 /* maxHosts */, 300 /* maxTTL */);
        auto clientBootstrap =
            std::make_shared<Aws::Crt::Io::ClientBootstrap>(eventLoopGroup, defaultHostResolver);
        clientBootstrap->EnableBlockingShutdown();
        return clientBootstrap;
    };
#endif
    return options;
}

static void ShutdownAWSSDK()
{
    Aws::Utils::Logging::ShutdownAWSLogging();
    Aws::ShutdownAPI(GetProverAWSSDKOptions());
}

static void InitSDK()
{
    Aws::InitAPI(GetProverAWSSDKOptions());
    std::atexit(ShutdownAWSSDK);
}

static std::once_flag shared_client_init_once_;

// Put this in a pointer so we can avoid initializing until after
// constructor is done calling AWS init.
// shared_client_ is thread safe.
void DoInitS3Client()
{
    std::call_once(aws_sdk_init_once_, InitSDK);

#if USE_AWS_CRT == 1
    auto s3_client_config = s3::ClientConfiguration();
    s3_client_config.throughputTargetGbps = 1;
    s3_client_config.partSize = 5000000;
#else
    // TODO: How many threads is enough?
    const auto num_threads = 400;
    auto s3_client_config = Aws::Client::ClientConfiguration();
    s3_client_config.executor = std::make_shared<S3Executor>(num_threads);
    s3_client_config.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(3);
    s3_client_config.maxConnections = 100;
    s3_client_config.disableExpectHeader = true;
    s3_client_config.verifySSL = false;
#endif

    s3_client_config.region = GetAwsRegion();
    CHECK_NE(s3_client_config.region, "", "Region must be set in ~/.aws/config");
    s3_client_config.scheme = Aws::Http::Scheme::HTTP;
    shared_client_ = std::make_unique<S3Client>(s3_client_config);
}

}  // namespace

std::unique_ptr<S3Client> shared_client_;
void InitS3Client() { std::call_once(shared_client_init_once_, DoInitS3Client); }

#else  // USE_MOCK_S3 != 1

std::unique_ptr<S3Client> shared_client_;
void InitS3Client() {}

#endif  // USE_MOCK_S3 != 1

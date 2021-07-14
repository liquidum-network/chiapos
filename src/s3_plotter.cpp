#include "s3_plotter.hpp"

#include "aws_globals.hpp"

namespace {
std::once_flag plotter_shared_client_init_once_;
void DoInitThreadPool(uint32_t num_threads)
{
    // Should be at least as large as the number of buckets.
    s3_plotter::shared_thread_pool_ = std::make_shared<S3Executor>(num_threads);
}
}  // namespace

#if USE_MOCK_S3 == 1

#include <chrono>
#include <random>
#include <thread>

namespace {
void DoInitS3Client(uint32_t num_threads) { DoInitThreadPool(num_threads); }
}  // namespace

#else  // USE_MOCK_S3 == 1

#include "aws/common/common.h"
#include "aws/common/logging.h"
#include "aws/core/client/RetryStrategy.h"
#include "aws/core/http/Scheme.h"
#include "aws/core/utils/logging/AWSLogging.h"
#include "aws/core/utils/logging/DefaultLogSystem.h"
#include "aws/core/utils/memory/AWSMemory.h"

namespace {

class S3PlotterRetryStrategy : public Aws::Client::RetryStrategy {
public:
    S3PlotterRetryStrategy(long max_retries = 10, long scale_factor = 25)
        : scale_factor_(scale_factor), max_retries_(max_retries)
    {
    }

    bool ShouldRetry(
        const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
        long attemptedRetries) const override final
    {
        if (attemptedRetries >= max_retries_) {
            return false;
        }
        if (error.GetErrorType() == Aws::Client::CoreErrors::REQUEST_TIMEOUT) {
            // We can't retry timeouts because the data is already gone.
            return false;
        }

        return error.ShouldRetry();
    }

    long CalculateDelayBeforeNextRetry(
        const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
        long attemptedRetries) const override final
    {
        if (attemptedRetries == 0) {
            return 0;
        }

        return (1 << attemptedRetries) * scale_factor_;
    }

    long GetMaxAttempts() const override final { return max_retries_ + 1; }

private:
    long scale_factor_;
    long max_retries_;
};

std::once_flag plotter_aws_sdk_init_once_;

Aws::SDKOptions GetPlotterAWSSDKOptions()
{
    Aws::SDKOptions options;
    const auto log_level = Aws::Utils::Logging::LogLevel::Warn;
    const std::string log_file_prefix = "plotter_s3_aws_sdk_";
    options.loggingOptions.logLevel = log_level;
    options.loggingOptions.logger_create_fn = [log_level,
                                               log_file_prefix = std::move(log_file_prefix)]() {
        const std::filesystem::path logdir = "./logs";
        if (std::filesystem::exists(logdir)) {
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
            eventLoopGroup, 200 /* maxHosts */, 300 /* maxTTL */);
        auto clientBootstrap =
            std::make_shared<Aws::Crt::Io::ClientBootstrap>(eventLoopGroup, defaultHostResolver);
        clientBootstrap->EnableBlockingShutdown();
        return clientBootstrap;
    };
#endif  // USE_AWS_CRT == 1

    options.httpOptions.installSigPipeHandler = true;
    options.httpOptions.httpClientFactory_create_fn = []() {
        return std::make_shared<S3PlotterHttpClientFactory>();
    };

    return options;
}

void ShutdownAWSSDK()
{
    s3_plotter::shared_thread_pool_->WaitForTasks();
    s3_plotter::shared_thread_pool_ = nullptr;
    Aws::Utils::Logging::ShutdownAWSLogging();
    Aws::ShutdownAPI(GetPlotterAWSSDKOptions());
}

void InitSDK()
{
    Aws::InitAPI(GetPlotterAWSSDKOptions());
    std::atexit(ShutdownAWSSDK);
}

void DoInitS3Client(uint32_t num_threads)
{
    std::call_once(plotter_aws_sdk_init_once_, InitSDK);
    DoInitThreadPool(num_threads);

#if USE_AWS_CRT == 1
    auto s3_client_config = s3::ClientConfiguration();
    s3_client_config.throughputTargetGbps = 10;
    s3_client_config.partSize = 100000000;
#else   // USE_AWS_CRT == 1
    auto s3_client_config = Aws::Client::ClientConfiguration();
    s3_client_config.retryStrategy = std::make_shared<S3PlotterRetryStrategy>(5);
    s3_client_config.maxConnections = 2 * num_threads;
    s3_client_config.disableExpectHeader = true;
    s3_client_config.requestTimeoutMs = 360000;
    s3_client_config.connectTimeoutMs = 20000;
    s3_client_config.verifySSL = false;
#endif  // USE_AWS_CRT == 1

    s3_client_config.executor = s3_plotter::shared_thread_pool_;
    s3_client_config.region = GetAwsRegion();
    CHECK_NE(s3_client_config.region, "", "Region must be set in ~/.aws/config");

    // The AWS SDK computes request payload hashes for all requests if the
    // scheme is HTTP
    s3_client_config.scheme = Aws::Http::Scheme::HTTPS;

    s3_plotter::shared_client_ = std::make_unique<S3NonSlicingClient>(
        s3_client_config, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, true);
}

}  // namespace

#endif  // USE_MOCK_S3 == 1

namespace s3_plotter {

void InitS3Client(uint32_t num_threads)
{
    std::call_once(plotter_shared_client_init_once_, DoInitS3Client, num_threads);
}

std::unique_ptr<S3NonSlicingClient> shared_client_;
std::shared_ptr<S3Executor> shared_thread_pool_;

}  // namespace s3_plotter

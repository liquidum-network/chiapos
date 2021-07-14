#ifndef SRC_CPP_S3_UTILS_HPP_
#define SRC_CPP_S3_UTILS_HPP_

#include <fmt/core.h>
#include <signal.h>

#include <future>
#include <string>
#include <utility>

#include "logging.hpp"
#include "preallocated_streambuf.hpp"
#include "s3_executor.hpp"

#if USE_MOCK_S3 == 1

#include <filesystem>

#ifndef MOCK_S3_SLEEP_TIME
#define MOCK_S3_SLEEP_TIME 200.0f
#endif  // MOCK_S3_SLEEP_TIME

#if MOCK_S3_SLEEP_TIME > 0
#define mock_s3_sleep_exponential()                                                               \
    do {                                                                                          \
        std::mt19937_64 eng{std::random_device{}()};                                              \
        std::exponential_distribution<float> dist{1.0f / static_cast<float>(MOCK_S3_SLEEP_TIME)}; \
        std::this_thread::sleep_for(std::chrono::milliseconds{static_cast<int>(dist(eng))});      \
    } while (false);
#else
#define mock_s3_sleep_exponential()
#endif

namespace s3 {
using PutObjectResponseReceivedHandler = void*;
using GetObjectResponseReceivedHandler = void*;
}  // namespace s3

namespace Aws {
namespace Client {
struct AsyncCallerContext {
    virtual ~AsyncCallerContext() = default;
};

}  // namespace Client
}  // namespace Aws

struct S3Client {
};

std::pair<int, bool> OpenMockS3File(const std::string& bucket, const std::string& key, bool write);
std::pair<std::string, int> OpenFile(const std::filesystem::path& path, bool write);
std::filesystem::path GetMockObjectPath(const std::string& bucket, const std::string& key);

std::string WriteBufferToS3(
    S3Client* s3_client,
    const std::string& bucket,
    const std::string& key,
    const uint8_t* buffer,
    size_t buffer_size);

#else  // USE_MOCK_S3

#include "aws/core/Aws.h"

#if USE_AWS_CRT == 1
#include "aws/s3-crt/S3CrtClient.h"
#include "aws/s3-crt/model/GetObjectRequest.h"
#include "aws/s3-crt/model/PutObjectRequest.h"
#include "aws/s3-crt/model/UploadPartRequest.h"
using StorageClass = s3::Model::StorageClass;
using S3Client = Aws::S3Crt::S3CrtClient;
namespace s3 = Aws::S3Crt;
static_assert(false, "Need to reimplemented S3Client PutObjectAsync");
#else
#include "aws/common/common.h"
#include "aws/core/http/curl/CurlHttpClient.h"
#include "aws/core/http/standard/StandardHttpRequest.h"
#include "aws/s3/S3Client.h"
#include "aws/s3/model/GetObjectRequest.h"
#include "aws/s3/model/PutObjectRequest.h"
#include "aws/s3/model/UploadPartRequest.h"
#include "curl_http_client.hpp"

namespace s3 = Aws::S3;
using S3Client = s3::S3Client;

namespace s3_plotter {
extern std::shared_ptr<S3Executor> shared_thread_pool_;
}

static void LogAndSwallowHandler(int signal)
{
    switch (signal) {
        case SIGPIPE:
            SPDLOG_DEBUG("Received SIGPIPE");
            break;
    }
}

class S3PlotterHttpClientFactory : public Aws::Http::HttpClientFactory {
public:
    std::shared_ptr<Aws::Http::HttpClient> CreateHttpClient(
        const Aws::Client::ClientConfiguration& clientConfiguration) const override
    {
        return std::make_shared<s3_plotter::HttpClient>(clientConfiguration);
    }

    std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(
        const Aws::String& uri,
        Aws::Http::HttpMethod method,
        const Aws::IOStreamFactory& streamFactory) const override
    {
        return Aws::Http::CreateHttpRequest(Aws::Http::URI(uri), method, streamFactory);
    }

    std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(
        const Aws::Http::URI& uri,
        Aws::Http::HttpMethod method,
        const Aws::IOStreamFactory& streamFactory) const override
    {
        auto request = std::make_shared<Aws::Http::Standard::StandardHttpRequest>(uri, method);
        request->SetResponseStreamFactory(streamFactory);

        return request;
    }

    void InitStaticState() override
    {
        s3_plotter::HttpClient::InitGlobalState();
        ::signal(SIGPIPE, LogAndSwallowHandler);
    }

    void CleanupStaticState() override { s3_plotter::HttpClient::CleanupGlobalState(); }
};

class StreamingUploadPartRequest : public s3::Model::UploadPartRequest {
public:
    bool IsEventStreamRequest() const override { return true; }

    ~StreamingUploadPartRequest() override {}

    StreamingUploadPartRequest() {}
    StreamingUploadPartRequest(const StreamingUploadPartRequest& other) = delete;
};

class StreamingPutObjectRequest : public s3::Model::PutObjectRequest {
public:
    bool IsEventStreamRequest() const override { return true; }

    ~StreamingPutObjectRequest() override {}

    StreamingPutObjectRequest() {}
    StreamingPutObjectRequest(const StreamingPutObjectRequest& other) = delete;
};

class S3NonSlicingClient : public S3Client {
public:
    S3NonSlicingClient(
        Aws::Client::ClientConfiguration config,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy signPayloads =
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        bool useVirtualAddressing = true)
        : S3Client(config, signPayloads, useVirtualAddressing)
    {
        DCHECK(config.executor.get() != nullptr);
        executor_ = static_cast<S3Executor*>(config.executor.get());
    }

    void PutObjectAsyncNoSlice(
        std::unique_ptr<StreamingPutObjectRequest> request,
        s3::PutObjectResponseReceivedHandler handler,
        std::shared_ptr<const Aws::Client::AsyncCallerContext> context) const
    {
        // HACK: The existing implementation slices PutObject, preventing us
        // from passing an overriding class.
        s3_plotter::shared_thread_pool_->SubmitTask([this,
                                                     request = std::move(request),
                                                     handler = std::move(handler),
                                                     context = std::move(context)]() {
            handler(this, *request, PutObject(*request), context);
        });
    }

    void UploadPartAsyncNoSlice(
        std::unique_ptr<StreamingUploadPartRequest> request,
        s3::UploadPartResponseReceivedHandler handler,
        std::shared_ptr<const Aws::Client::AsyncCallerContext> context) const
    {
        // HACK: The existing implementation slices PutObject, preventing us
        // from passing an overriding class.
        s3_plotter::shared_thread_pool_->SubmitTask([this,
                                                     request = std::move(request),
                                                     handler = std::move(handler),
                                                     context = std::move(context)]() {
            handler(this, *request, UploadPart(*request), context);
        });
    }

    ~S3NonSlicingClient() override {}

protected:
    S3Executor* executor_;
};

#endif

using StorageClass = s3::Model::StorageClass;

// https://github.com/apache/arrow/blob/master/cpp/src/arrow/filesystem/s3fs.cc#L383-L392
// A non-copying iostream.
// See https://stackoverflow.com/questions/35322033/aws-c-sdk-uploadpart-times-out
// https://stackoverflow.com/questions/13059091/creating-an-input-stream-from-constant-memory
class StringViewStream : Aws::Utils::Stream::PreallocatedStreamBuf, public std::iostream {
public:
    StringViewStream(const void* data, int64_t nbytes)
        : Aws::Utils::Stream::PreallocatedStreamBuf(
              reinterpret_cast<unsigned char*>(const_cast<void*>(data)),
              static_cast<size_t>(nbytes)),
          std::iostream(this)
    {
    }

    virtual ~StringViewStream() override {}
};

inline Aws::IOStreamFactory AwsWriteableStreamFactory(void* data, int64_t nbytes)
{
    return [=]() { return Aws::New<StringViewStream>("", data, nbytes); };
}

std::string WriteBufferToS3(
    S3Client* s3_client,
    const std::string& bucket,
    const std::string& key,
    const uint8_t* buffer,
    size_t buffer_size);

#endif  // USE_MOCK_S3

inline bool is_file_uri(const std::string_view& uri) { return uri.rfind("file://", 0) == 0; }

inline std::string parse_file_uri(const std::string_view& uri)
{
    DCHECK_EQ(uri.rfind("file://", 0), 0);
    // skip s3://
    constexpr auto prefix_size = 7;
    return std::string(uri.substr(prefix_size));
}

// returns (bucket, path)
inline std::pair<std::string, std::string> parse_s3_uri(const std::string_view& uri)
{
    CHECK_EQ(uri.rfind("s3://", 0), 0, "s3 uri is required");
    // skip s3://
    constexpr auto prefix_size = 5;
    const auto bucket_end = uri.find('/', prefix_size);
    CHECK_NE(bucket_end, std::string::npos);
    return {
        std::string{uri.substr(prefix_size, bucket_end - prefix_size)},
        std::string{uri.substr(bucket_end + 1)}};
}

inline std::string GetHttpRange(size_t offset, size_t bytes_to_read)
{
    return "bytes=" + std::to_string(offset) + '-' + std::to_string(offset + bytes_to_read - 1);
}

#endif  // SRC_CPP_S3_UTILS_HPP_

#pragma once

#include <aws/core/Core_EXPORTS.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpClient.h>
#include <aws/core/http/curl/CurlHandleContainer.h>
#include <aws/core/http/curl/CurlHttpClient.h>
#include <aws/core/utils/memory/stl/AWSString.h>

#include <atomic>
#include <memory>

namespace s3_plotter {

class HttpClient : public Aws::Http::HttpClient {
public:
    using Base = HttpClient;

    // Creates client, initializes curl handle if it hasn't been created already.
    HttpClient(const Aws::Client::ClientConfiguration& clientConfig);

    // Makes request and receives response synchronously
    std::shared_ptr<Aws::Http::HttpResponse> MakeRequest(
        const std::shared_ptr<Aws::Http::HttpRequest>& request,
        Aws::Utils::RateLimits::RateLimiterInterface* readLimiter = nullptr,
        Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter = nullptr) const override;

    static void InitGlobalState() { Aws::Http::CurlHttpClient::InitGlobalState(); }
    static void CleanupGlobalState() { Aws::Http::CurlHttpClient::CleanupGlobalState(); }

private:
    mutable Aws::Http::CurlHandleContainer m_curlHandleContainer;
    bool m_disableExpectHeader;
    bool m_allowRedirects;
};

}  // namespace s3_plotter

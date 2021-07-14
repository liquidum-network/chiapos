#include "curl_http_client.hpp"

#include <aws/common/common.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/curl/CurlHttpClient.h>
#include <aws/core/http/standard/StandardHttpResponse.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/ratelimiter/RateLimiterInterface.h>

#include <algorithm>
#include <cassert>

#include "logging.hpp"
#include "time_helpers.hpp"

using namespace Aws::Client;
using namespace Aws::Http;
using namespace Aws::Http::Standard;
using namespace Aws::Utils;

namespace s3_plotter {
namespace {

void SetOptCodeForHttpMethod(CURL* requestHandle, const std::shared_ptr<HttpRequest>& request)
{
    switch (request->GetMethod()) {
        case HttpMethod::HTTP_GET:
            curl_easy_setopt(requestHandle, CURLOPT_HTTPGET, 1L);
            break;
        case HttpMethod::HTTP_POST:
            if (request->HasHeader(Aws::Http::CONTENT_LENGTH_HEADER) &&
                request->GetHeaderValue(Aws::Http::CONTENT_LENGTH_HEADER) == "0") {
                curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "POST");
            } else {
                curl_easy_setopt(requestHandle, CURLOPT_POST, 1L);
            }
            break;
        case HttpMethod::HTTP_PUT:
            if ((!request->HasHeader(Aws::Http::CONTENT_LENGTH_HEADER) ||
                 request->GetHeaderValue(Aws::Http::CONTENT_LENGTH_HEADER) == "0") &&
                !request->HasHeader(Aws::Http::TRANSFER_ENCODING_HEADER)) {
                curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "PUT");
            } else {
                curl_easy_setopt(requestHandle, CURLOPT_PUT, 1L);
            }
            break;
        case HttpMethod::HTTP_HEAD:
            curl_easy_setopt(requestHandle, CURLOPT_HTTPGET, 1L);
            curl_easy_setopt(requestHandle, CURLOPT_NOBODY, 1L);
            break;
        case HttpMethod::HTTP_PATCH:
            if ((!request->HasHeader(Aws::Http::CONTENT_LENGTH_HEADER) ||
                 request->GetHeaderValue(Aws::Http::CONTENT_LENGTH_HEADER) == "0") &&
                !request->HasHeader(Aws::Http::TRANSFER_ENCODING_HEADER)) {
                curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "PATCH");
            } else {
                curl_easy_setopt(requestHandle, CURLOPT_POST, 1L);
                curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "PATCH");
            }

            break;
        case HttpMethod::HTTP_DELETE:
            curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "DELETE");
            break;
        default:
            assert(0);
            curl_easy_setopt(requestHandle, CURLOPT_CUSTOMREQUEST, "GET");
            break;
    }
}

struct CurlWriteCallbackContext {
    CurlWriteCallbackContext(const HttpClient* client, HttpRequest* request, HttpResponse* response)
        : m_client(client), m_request(request), m_response(response), m_numBytesResponseReceived(0)
    {
    }

    const HttpClient* m_client;
    HttpRequest* const m_request;
    HttpResponse* const m_response;
    int64_t m_numBytesResponseReceived;
};

struct CurlReadCallbackContext {
    CurlReadCallbackContext(const HttpClient* client, CURL* curlHandle, HttpRequest* request)
        : m_client(client), m_curlHandle(curlHandle), m_request(request)
    {
    }

    const HttpClient* const m_client;
    CURL* const m_curlHandle;
    HttpRequest* m_request;
};

size_t ReadBody(char* ptr, size_t size, size_t nmemb, void* userdata)
{
    CurlReadCallbackContext* context = reinterpret_cast<CurlReadCallbackContext*>(userdata);
    if (context == nullptr) {
        SPDLOG_TRACE("ReadBody return early no context");
        return 0;
    }

    const HttpClient* client = context->m_client;
    if (!client->ContinueRequest(*context->m_request) || !client->IsRequestProcessingEnabled()) {
        SPDLOG_TRACE("ReadBody return early abort");
        return CURL_READFUNC_ABORT;
    }

    HttpRequest* request = context->m_request;
    const std::shared_ptr<Aws::IOStream>& stream = request->GetContentBody();
    if (stream == nullptr) {
        SPDLOG_TRACE("ReadBody return early no stream");
        return 0;
    }

    ssize_t max_to_read = size * nmemb;
    if (max_to_read == 0) {
        SPDLOG_TRACE("ReadBody return early no max bytes to read");
        return 0;
    }

    const auto before_read_time = get_coarse_now_time_nanos();
    const auto bytes_read = stream->rdbuf()->sgetn(ptr, max_to_read);
    const auto after_read_time = get_coarse_now_time_nanos();

    constexpr uint64_t read_time_log_threshold_nanos = 5ULL * 1000 * 1000 * 1000;
    if (after_read_time - before_read_time > read_time_log_threshold_nanos) {
        SPDLOG_DEBUG(
            "reading {}/{} bytes took {}s on {}",
            bytes_read,
            max_to_read,
            (after_read_time - before_read_time) / 1e9,
            request->GetUri().GetURIString());
    }
    return bytes_read;
}

size_t WriteData(char* ptr, size_t size, size_t nmemb, void* userdata)
{
    if (ptr == nullptr) {
        return 0;
    }
    CurlWriteCallbackContext* context = reinterpret_cast<CurlWriteCallbackContext*>(userdata);

    const HttpClient* client = context->m_client;
    if (!client->ContinueRequest(*context->m_request) || !client->IsRequestProcessingEnabled()) {
        return 0;
    }

    HttpResponse* response = context->m_response;
    auto& stream = response->GetResponseBody();
    size_t sizeToWrite = size * nmemb;
    stream.write(ptr, static_cast<std::streamsize>(sizeToWrite));
    if (stream.fail()) {
        SPDLOG_ERROR(
            "failure writing to buffer on {}", context->m_request->GetUri().GetURIString());
        return 0;
    }
    context->m_numBytesResponseReceived += sizeToWrite;
    return sizeToWrite;
}

size_t SeekBody(void* userdata, curl_off_t offset, int origin)
{
    CurlReadCallbackContext* context = reinterpret_cast<CurlReadCallbackContext*>(userdata);
    if (context == nullptr) {
        return CURL_SEEKFUNC_FAIL;
    }

    const HttpClient* client = context->m_client;
    if (!client->ContinueRequest(*context->m_request) || !client->IsRequestProcessingEnabled()) {
        return CURL_SEEKFUNC_FAIL;
    }

    HttpRequest* request = context->m_request;
    const std::shared_ptr<Aws::IOStream>& ioStream = request->GetContentBody();

    std::ios_base::seekdir dir;
    switch (origin) {
        case SEEK_SET:
            dir = std::ios_base::beg;
            break;
        case SEEK_CUR:
            dir = std::ios_base::cur;
            break;
        case SEEK_END:
            dir = std::ios_base::end;
            break;
        default:
            return CURL_SEEKFUNC_FAIL;
    }

    ioStream->clear();
    ioStream->seekg(offset, dir);
    if (ioStream->fail()) {
        return CURL_SEEKFUNC_CANTSEEK;
    }

    return CURL_SEEKFUNC_OK;
}

size_t WriteHeader(char* ptr, size_t size, size_t nmemb, void* userdata)
{
    if (ptr) {
        CurlWriteCallbackContext* context = reinterpret_cast<CurlWriteCallbackContext*>(userdata);
        HttpResponse* response = context->m_response;
        Aws::String headerLine(ptr);
        Aws::Vector<Aws::String> keyValuePair = StringUtils::Split(headerLine, ':', 2);

        if (keyValuePair.size() == 2) {
            response->AddHeader(
                StringUtils::Trim(keyValuePair[0].c_str()),
                StringUtils::Trim(keyValuePair[1].c_str()));
        }

        return size * nmemb;
    }
    return 0;
}

void OverrideOptionsOnConnectionHandle(CURL* handle)
{
    curl_easy_setopt(handle, CURLOPT_BUFFERSIZE, 64 * 1024);
    curl_easy_setopt(handle, CURLOPT_UPLOAD_BUFFERSIZE, 64 * 1024);
    curl_easy_setopt(handle, CURLOPT_DNS_CACHE_TIMEOUT, 3600L);
}

std::string ReplaceHttpWithHttp(std::string original_url)
{
    if (original_url.substr(0, 8) == "https://") {
        return "http://" + original_url.substr(8);
    } else {
        return original_url;
    }
}

}  // namespace

HttpClient::HttpClient(const Aws::Client::ClientConfiguration& clientConfig)
    : m_curlHandleContainer(
          clientConfig.maxConnections,
          clientConfig.httpRequestTimeoutMs,
          clientConfig.connectTimeoutMs,
          clientConfig.enableTcpKeepAlive,
          clientConfig.tcpKeepAliveIntervalMs,
          clientConfig.requestTimeoutMs,
          clientConfig.lowSpeedLimit),
      m_disableExpectHeader(clientConfig.disableExpectHeader)
{
    if (clientConfig.followRedirects == FollowRedirectsPolicy::NEVER ||
        (clientConfig.followRedirects == FollowRedirectsPolicy::DEFAULT &&
         clientConfig.region == Aws::Region::AWS_GLOBAL)) {
        m_allowRedirects = false;
    } else {
        m_allowRedirects = true;
    }
}
std::shared_ptr<HttpResponse> HttpClient::MakeRequest(
    const std::shared_ptr<HttpRequest>& request,
    Aws::Utils::RateLimits::RateLimiterInterface* readLimiter,
    Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter) const
{
    URI uri = request->GetUri();
    Aws::String url = ReplaceHttpWithHttp(uri.GetURIString());
    std::shared_ptr<HttpResponse> response = std::make_shared<StandardHttpResponse>(request);

    struct curl_slist* headers = NULL;

    Aws::StringStream headerStream;
    HeaderValueCollection requestHeaders = request->GetHeaders();

    for (auto& requestHeader : requestHeaders) {
        headerStream.str("");
        headerStream << requestHeader.first << ": " << requestHeader.second;
        Aws::String headerString = headerStream.str();
        headers = curl_slist_append(headers, headerString.c_str());
    }

    if (!request->HasHeader(Aws::Http::TRANSFER_ENCODING_HEADER)) {
        headers = curl_slist_append(headers, "transfer-encoding:");
    }

    if (!request->HasHeader(Aws::Http::CONTENT_LENGTH_HEADER)) {
        headers = curl_slist_append(headers, "content-length:");
    }

    if (!request->HasHeader(Aws::Http::CONTENT_TYPE_HEADER)) {
        headers = curl_slist_append(headers, "content-type:");
    }

    // Discard Expect header so as to avoid using multiple payloads to send a http request (header +
    // body)
    if (m_disableExpectHeader) {
        headers = curl_slist_append(headers, "Expect:");
    }

    CURL* connectionHandle = m_curlHandleContainer.AcquireCurlHandle();
    if (connectionHandle) {
        if (headers) {
            curl_easy_setopt(connectionHandle, CURLOPT_HTTPHEADER, headers);
        }

        CurlWriteCallbackContext writeContext(this, request.get(), response.get());
        CurlReadCallbackContext readContext(this, connectionHandle, request.get());

        SetOptCodeForHttpMethod(connectionHandle, request);

        curl_easy_setopt(connectionHandle, CURLOPT_URL, url.c_str());
        curl_easy_setopt(connectionHandle, CURLOPT_WRITEFUNCTION, WriteData);
        curl_easy_setopt(connectionHandle, CURLOPT_WRITEDATA, &writeContext);
        curl_easy_setopt(connectionHandle, CURLOPT_HEADERFUNCTION, WriteHeader);
        curl_easy_setopt(connectionHandle, CURLOPT_HEADERDATA, &writeContext);

        curl_easy_setopt(connectionHandle, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(connectionHandle, CURLOPT_SSL_VERIFYHOST, 0L);

        if (m_allowRedirects) {
            curl_easy_setopt(connectionHandle, CURLOPT_FOLLOWLOCATION, 1L);
        } else {
            curl_easy_setopt(connectionHandle, CURLOPT_FOLLOWLOCATION, 0L);
        }
        curl_easy_setopt(connectionHandle, CURLOPT_PROXY, "");

        if (request->GetContentBody()) {
            curl_easy_setopt(connectionHandle, CURLOPT_READFUNCTION, ReadBody);
            curl_easy_setopt(connectionHandle, CURLOPT_READDATA, &readContext);
            curl_easy_setopt(connectionHandle, CURLOPT_SEEKFUNCTION, SeekBody);
            curl_easy_setopt(connectionHandle, CURLOPT_SEEKDATA, &readContext);
        }

        OverrideOptionsOnConnectionHandle(connectionHandle);
        SPDLOG_DEBUG("Starting request on {}", url);
        CURLcode curlResponseCode = curl_easy_perform(connectionHandle);
        bool shouldContinueRequest = ContinueRequest(*request);
        if (curlResponseCode != CURLE_OK && shouldContinueRequest) {
            response->SetClientErrorType(CoreErrors::NETWORK_CONNECTION);
            Aws::StringStream ss;
            ss << "curlCode: " << curlResponseCode << ", " << curl_easy_strerror(curlResponseCode);
            if (curlResponseCode != 6) {
                SPDLOG_WARN("error on {}: {}", url, ss.str());
            }
            response->SetClientErrorMessage(ss.str());
        } else if (!shouldContinueRequest) {
            response->SetClientErrorType(CoreErrors::USER_CANCELLED);
            response->SetClientErrorMessage("Request cancelled by user's continuation handler");
        } else {
            long responseCode;
            curl_easy_getinfo(connectionHandle, CURLINFO_RESPONSE_CODE, &responseCode);
            response->SetResponseCode(static_cast<HttpResponseCode>(responseCode));
            if (responseCode >= 400) {
                SPDLOG_WARN("response on {}: {}", url, responseCode);
            }

            char* contentType = nullptr;
            curl_easy_getinfo(connectionHandle, CURLINFO_CONTENT_TYPE, &contentType);
            if (contentType) {
                response->SetContentType(contentType);
            }

            if (request->GetMethod() != HttpMethod::HTTP_HEAD &&
                writeContext.m_client->IsRequestProcessingEnabled() &&
                response->HasHeader(Aws::Http::CONTENT_LENGTH_HEADER)) {
                const Aws::String& contentLength =
                    response->GetHeader(Aws::Http::CONTENT_LENGTH_HEADER);
                int64_t numBytesResponseReceived = writeContext.m_numBytesResponseReceived;
                if (StringUtils::ConvertToInt64(contentLength.c_str()) !=
                    numBytesResponseReceived) {
                    response->SetClientErrorType(CoreErrors::NETWORK_CONNECTION);
                    response->SetClientErrorMessage(
                        "Response body length doesn't match the content-length header.");
                }
            }
        }

        if (curlResponseCode != CURLE_OK) {
            m_curlHandleContainer.DestroyCurlHandle(connectionHandle);
        } else {
            m_curlHandleContainer.ReleaseCurlHandle(connectionHandle);
        }
        // go ahead and flush the response body stream
        response->GetResponseBody().flush();
    }
    if (headers) {
        curl_slist_free_all(headers);
    }

    return response;
}

}  // namespace s3_plotter

#include <aws/core/Aws.h>
#include <unistd.h>

#include <sstream>
#include <thread>

#define USE_AWS_CRT 1

#if USE_AWS_CRT == 1
#include "aws/s3-crt/S3CrtClient.h"
#include "aws/s3-crt/model/PutObjectRequest.h"
using S3Client = Aws::S3Crt::S3CrtClient;
namespace s3 = Aws::S3Crt;
using ClientConfiguration = s3::ClientConfiguration;
#else
#include "aws/s3/S3Client.h"
#include "aws/s3/model/PutObjectRequest.h"
namespace s3 = Aws::S3;
using S3Client = Aws::S3::S3Client;
using ClientConfiguration = Aws::Client::ClientConfiguration;
#endif

void Callback(
    const S3Client* s3_client,
    const s3::Model::PutObjectRequest& request,
    const s3::Model::PutObjectOutcome& outcome,
    const std::shared_ptr<const Aws::Client::AsyncCallerContext>& context)
{
}

void DoPutObject(S3Client* s3_client)
{
    auto request =
        s3::Model::PutObjectRequest().WithBucket("my-bucket").WithKey("putobject-example-test-key");
    request.SetBody(std::make_shared<std::stringstream>("test"));
    s3_client->PutObjectAsync(request, Callback, nullptr);
}

int main()
{
    Aws::InitAPI(Aws::SDKOptions{});
    S3Client s3_client(ClientConfiguration{});

    std::thread(DoPutObject, &s3_client).join();

    std::cout << "waiting" << std::endl;
    usleep(2000 * 1000);
    std::cout << "done waiting" << std::endl;

    Aws::ShutdownAPI(Aws::SDKOptions{});
}

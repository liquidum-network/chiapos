#pragma once

#if USE_MOCK_S3 == 1

#include <streambuf>

namespace Aws {
namespace Utils {
namespace Stream {

class PreallocatedStreamBuf : public std::streambuf {
public:
    PreallocatedStreamBuf(unsigned char* buffer, uint64_t lengthToRead)
        : m_underlyingBuffer(buffer), m_lengthToRead(lengthToRead)
    {
        char* end = reinterpret_cast<char*>(m_underlyingBuffer + m_lengthToRead);
        char* begin = reinterpret_cast<char*>(m_underlyingBuffer);
        setp(begin, end);
        setg(begin, begin, end);
    }

    PreallocatedStreamBuf(const PreallocatedStreamBuf&) = delete;
    PreallocatedStreamBuf& operator=(const PreallocatedStreamBuf&) = delete;

    PreallocatedStreamBuf(PreallocatedStreamBuf&& toMove) = delete;
    PreallocatedStreamBuf& operator=(PreallocatedStreamBuf&&) = delete;

    unsigned char* GetBuffer() { return m_underlyingBuffer; }

protected:
    // pos_type seekoff(
    // off_type off,
    // std::ios_base::seekdir dir,
    // std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) override;
    // pos_type seekpos(
    // pos_type pos,
    // std::ios_base::openmode which = std::ios_base::in | std::ios_base::out) override;

private:
    unsigned char* m_underlyingBuffer;
    const uint64_t m_lengthToRead;
};
}  // namespace Stream
}  // namespace Utils
}  // namespace Aws

#else  // USE_MOCK_S3 == 1

#include "aws/core/utils/stream/PreallocatedStreamBuf.h"

#endif  // USE_MOCK_S3 == 1

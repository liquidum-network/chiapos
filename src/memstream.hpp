#pragma once

#include <istream>
#include <streambuf>

// https://tuttlem.github.io/2014/08/18/getting-istream-to-work-off-a-byte-array.html

namespace internal {
class membuf : public std::basic_streambuf<char> {
public:
    membuf(const uint8_t* p, size_t l) { setg((char*)p, (char*)p, (char*)p + l); }
};

}  // namespace internal

class memstream : public std::istream {
public:
    memstream(const uint8_t* p, size_t l) : std::istream(&buffer_), buffer_(p, l)
    {
        rdbuf(&buffer_);
    }

private:
    internal::membuf buffer_;
};

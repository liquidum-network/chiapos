#ifndef SRC_CPP_PRINT_HELPERS_HPP_
#define SRC_CPP_PRINT_HELPERS_HPP_

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <filesystem>
#include <string>

template <typename T>
std::ostream& operator<<(std::ostream& out, const std::vector<T>& v)
{
    out << "{";
    size_t last = v.size() - 1;
    for (size_t i = 0; i < v.size(); ++i) {
        out << v[i];
        if (i != last)
            out << ", ";
    }
    out << "}";
    return out;
}

namespace fmt {
template <>
struct formatter<std::filesystem::path> : formatter<std::string> {
};
}  // namespace fmt

#endif  // SRC_CPP_PRINT_HELPERS_HPP_

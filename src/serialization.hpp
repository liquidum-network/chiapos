#ifndef SRC_CPP_SERIALIZATION_HPP_
#define SRC_CPP_SERIALIZATION_HPP_

#include <array>
#include <cstdlib>
#include <iomanip>
#include <iterator>
#include <string>
#include <vector>

inline std::vector<uint8_t> HexToBytes(const std::string &hex)
{
    std::vector<uint8_t> result;
    result.reserve(hex.size() / 2);
    for (uint32_t i = 0; i < hex.length(); i += 2) {
        std::string byteString = hex.substr(i, 2);
        uint8_t byte = (uint8_t)strtol(byteString.c_str(), NULL, 16);
        result.push_back(byte);
    }
    return result;
}

// https://stackoverflow.com/a/55364414/1301879
inline std::string RawBytesToHex(const uint8_t *input_data, size_t input_size)
{
    static const char characters[] = "0123456789abcdef";
    std::string ret(input_size * 2, 0);
    char *buf = const_cast<char *>(ret.data());

    for (size_t i = 0; i < input_size; ++i) {
        uint8_t input_byte = input_data[i];
        *buf++ = characters[input_byte >> 4];
        *buf++ = characters[input_byte & 0x0F];
    }
    return ret;
}

inline std::string BytesToHex(const std::vector<uint8_t> &input)
{
    return RawBytesToHex(input.data(), input.size());
}

inline std::string Strip0x(const std::string &hex)
{
    if (hex.size() > 1 && (hex.substr(0, 2) == "0x" || hex.substr(0, 2) == "0X")) {
        return hex.substr(2);
    }
    return hex;
}

template <typename T, typename Iter, std::size_t... Is>
constexpr auto to_array(Iter &iter, std::index_sequence<Is...>) -> std::array<T, sizeof...(Is)>
{
    return {{((void)Is, *iter++)...}};
}

template <
    std::size_t N,
    typename Iter,
    typename T = typename std::iterator_traits<Iter>::value_type>
constexpr auto to_array(Iter iter) -> std::array<T, N>
{
    return to_array<T>(iter, std::make_index_sequence<N>{});
}

#endif  // SRC_CPP_SERIALIZATION_HPP_

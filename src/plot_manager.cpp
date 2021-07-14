#include "plot_manager.hpp"

#include <filesystem>
#include <fstream>
#include <optional>

std::optional<json> TryReadJsonFromFile(const std::string& filename)
{
    std::ifstream file(filename, std::ios_base::in | std::ios_base::binary);
    try {
        return json::parse(file);
    } catch (json::parse_error&) {
        // pass
    }
    return {};
}

void WriteJsonToFile(const std::string& filename, const json& input_json)
{
    const auto tmp_filename = filename + ".tmp";
    {
        std::ofstream file(
            tmp_filename, std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
        file << input_json;
    }

    std::filesystem::rename(tmp_filename, filename);
}

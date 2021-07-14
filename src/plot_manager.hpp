#ifndef SRC_CPP_PLOT_MANAGER_HPP_
#define SRC_CPP_PLOT_MANAGER_HPP_

#include <optional>

#include "nlohmann/json.hpp"

using json = nlohmann::json;

std::optional<json> TryReadJsonFromFile(const std::string& filename);

void WriteJsonToFile(const std::string& filename, const json& input_json);

#endif  // SRC_CPP_PLOT_MANAGER_HPP_

#pragma once
// Cascade :: core::config
//
// Small configuration manager: loads key=value pairs from a file, allows
// environment variables to override file values (env > file), and exposes
// typed getters with defaults so call sites never hand-roll parsing.

#include <charconv>
#include <cstdlib>
#include <fstream>
#include <string>
#include <unordered_map>

namespace cascade::core {

class ConfigManager {
public:
    ConfigManager() = default;

    // Loads a "key=value" file. Blank lines and lines starting with '#'
    // are ignored. Returns false if the file could not be opened.
    bool load_file(const std::string& path) {
        std::ifstream file(path);
        if (!file.is_open()) return false;

        std::string line;
        while (std::getline(file, line)) {
            auto trimmed = trim(line);
            if (trimmed.empty() || trimmed[0] == '#') continue;

            auto eq = trimmed.find('=');
            if (eq == std::string::npos) continue;

            std::string key = trim(trimmed.substr(0, eq));
            std::string value = trim(trimmed.substr(eq + 1));
            values_[key] = value;
        }
        return true;
    }

    void set(const std::string& key, const std::string& value) {
        values_[key] = value;
    }

    // Precedence: environment variable > loaded file value > default.
    std::string get_string(const std::string& key, const std::string& default_value = "") const {
        if (const char* env = std::getenv(key.c_str())) return std::string(env);
        auto it = values_.find(key);
        if (it != values_.end()) return it->second;
        return default_value;
    }

    int get_int(const std::string& key, int default_value = 0) const {
        auto raw = get_string(key);
        if (raw.empty()) return default_value;
        int result{};
        auto [ptr, ec] = std::from_chars(raw.data(), raw.data() + raw.size(), result);
        (void)ptr;
        return ec == std::errc{} ? result : default_value;
    }

    bool get_bool(const std::string& key, bool default_value = false) const {
        auto raw = get_string(key);
        if (raw.empty()) return default_value;
        for (auto& c : raw) c = static_cast<char>(::tolower(c));
        if (raw == "1" || raw == "true" || raw == "yes" || raw == "on") return true;
        if (raw == "0" || raw == "false" || raw == "no" || raw == "off") return false;
        return default_value;
    }

    double get_double(const std::string& key, double default_value = 0.0) const {
        auto raw = get_string(key);
        if (raw.empty()) return default_value;
        try {
            return std::stod(raw);
        } catch (...) {
            return default_value;
        }
    }

    bool has(const std::string& key) const {
        if (std::getenv(key.c_str())) return true;
        return values_.contains(key);
    }

private:
    static std::string trim(const std::string& s) {
        auto start = s.find_first_not_of(" \t\r\n");
        if (start == std::string::npos) return "";
        auto end = s.find_last_not_of(" \t\r\n");
        return s.substr(start, end - start + 1);
    }

    std::unordered_map<std::string, std::string> values_;
};

} // namespace cascade::core
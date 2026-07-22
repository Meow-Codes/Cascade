// tests/test_config.cpp
#include <gtest/gtest.h>
#include <fstream>
#include "config/config.hpp"
#ifdef _WIN32

inline void set_test_env(const char* key, const char* value) {
    _putenv_s(key, value);
}

inline void unset_test_env(const char* key) {
    _putenv_s(key, "");
}
#else
inline void set_test_env(const char* key, const char* value) {
    setenv(key, value, 1);
}

inline void unset_test_env(const char* key) {
    unsetenv(key);
}
#endif

using cascade::core::ConfigManager;

TEST(ConfigManager, LoadsFileValues) {
    const char* path = "cascade_test_config.conf";
    std::ofstream f(path);
    ASSERT_TRUE(f.is_open());
    f << "# comment\n\nport=9092\nname = broker-1\nenabled=true\n";
    f.close();

    ConfigManager cfg;
    ASSERT_TRUE(cfg.load_file(path));
    EXPECT_EQ(cfg.get_int("port", -1), 9092);
    EXPECT_EQ(cfg.get_string("name"), "broker-1");
    EXPECT_TRUE(cfg.get_bool("enabled"));
}

TEST(ConfigManager, EnvOverridesFile) {
    set_test_env("CASCADE_TEST_KEY", "from_env");
    ConfigManager cfg;
    cfg.set("CASCADE_TEST_KEY", "from_file");
    EXPECT_EQ(cfg.get_string("CASCADE_TEST_KEY"), "from_env");
    unset_test_env("CASCADE_TEST_KEY");
}

TEST(ConfigManager, DefaultsWhenMissing) {
    ConfigManager cfg;
    EXPECT_EQ(cfg.get_int("missing", 42), 42);
    EXPECT_EQ(cfg.get_string("missing", "fallback"), "fallback");
}
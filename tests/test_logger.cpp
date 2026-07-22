// tests/test_logger.cpp
#include <gtest/gtest.h>
#include "logger/logger.hpp"

using cascade::core::Logger;
using cascade::core::LogLevel;

TEST(Logger, DoesNotCrashUnderConcurrentLogging) {
    Logger::instance().set_min_level(LogLevel::Trace);
    std::vector<std::thread> threads;
    for (int t = 0; t < 8; ++t) {
        threads.emplace_back([t] {
            for (int i = 0; i < 1000; ++i) {
                CASCADE_LOG_INFO("thread {} iteration {}", t, i);
            }
        });
    }
    for (auto& th : threads) th.join();
    Logger::instance().flush();
    SUCCEED();
}
set(TEST_SOURCES
    test_logger.cpp
    test_config.cpp
    test_memory_pool.cpp
    test_spsc_queue.cpp
    test_mpmc_queue.cpp
    test_thread_pool.cpp
    test_timer_wheel.cpp
    test_framing.cpp
    test_rate_limiter.cpp
    test_segment.cpp
    test_log_recovery.cpp
    test_storage_property.cpp
    test_storage_concurrent_readers.cpp
)

# Linux/POSIX networking tests
if(UNIX AND NOT WIN32)
    list(APPEND TEST_SOURCES
        test_tcp.cpp
        test_udp.cpp
        test_connection_manager.cpp
    )
endif()

add_executable(cascade_tests ${TEST_SOURCES})

target_link_libraries(cascade_tests PRIVATE cascade_core gtest_main)
target_include_directories(cascade_tests PRIVATE ${CMAKE_SOURCE_DIR}/cpp/core)

include(GoogleTest)
gtest_add_tests(TARGET cascade_tests)
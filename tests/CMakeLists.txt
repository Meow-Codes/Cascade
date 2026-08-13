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
    test_broker_ordering.cpp
    test_broker_consumer_group.cpp
    test_broker_backpressure.cpp
    test_metrics_registry.cpp
    test_metrics_http_server.cpp
    test_tracing.cpp
    test_metrics_overhead.cpp
    test_audio_packet_codec.cpp
    test_silence_detector.cpp
    test_retransmit_cache.cpp
    test_priority_queue.cpp
    test_jitter_buffer.cpp
    test_audio_session_udp.cpp
    test_network_estimator.cpp
    test_bitrate_controller.cpp
    test_adaptive_sender.cpp
    test_io_uring_loop.cpp
    test_zero_copy_transfer.cpp
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
add_executable(cascade_integration_tests test_broker_control_plane_integration.cpp)

target_link_libraries(cascade_tests PRIVATE cascade_core gtest_main)
target_include_directories(cascade_tests PRIVATE ${CMAKE_SOURCE_DIR}/cpp/core)
target_link_libraries(cascade_integration_tests PRIVATE cascade_core gtest_main)
target_include_directories(cascade_integration_tests PRIVATE ${CMAKE_SOURCE_DIR}/cpp/core)
# Deliberately NOT calling gtest_discover_tests() here -- this keeps it
# out of the default `ctest` run, which must stay runnable with zero
# external dependencies (Go server, network) per every prior phase's
# testing philosophy in this project.

include(GoogleTest)
gtest_add_tests(TARGET cascade_tests)
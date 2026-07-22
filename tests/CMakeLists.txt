add_executable(cascade_tests
  test_logger.cpp
  test_config.cpp
  test_memory_pool.cpp
  test_spsc_queue.cpp
  test_mpmc_queue.cpp
  test_thread_pool.cpp
  test_timer_wheel.cpp
)

target_link_libraries(cascade_tests PRIVATE cascade_core gtest_main)
target_include_directories(cascade_tests PRIVATE ${CMAKE_SOURCE_DIR}/cpp/core)

include(GoogleTest)
gtest_add_tests(TARGET cascade_tests)
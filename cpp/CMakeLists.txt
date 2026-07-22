add_library(cascade_core INTERFACE)

target_include_directories(cascade_core INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/core
)

target_compile_features(cascade_core INTERFACE cxx_std_23)

find_package(Threads REQUIRED)
target_link_libraries(cascade_core INTERFACE Threads::Threads)
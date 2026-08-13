add_library(cascade_core INTERFACE)

target_include_directories(cascade_core INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/core
)

target_compile_features(cascade_core INTERFACE cxx_std_23)

find_package(Threads REQUIRED)
target_link_libraries(cascade_core INTERFACE Threads::Threads)

add_subdirectory(core/controlplane)
target_link_libraries(cascade_core INTERFACE cascade_controlplane_client)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING REQUIRED liburing)
target_link_libraries(my_target PRIVATE ${LIBURING_LIBRARIES})
target_include_directories(my_target PRIVATE ${LIBURING_INCLUDE_DIRS})
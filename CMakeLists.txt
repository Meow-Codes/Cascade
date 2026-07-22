cmake_minimum_required(VERSION 3.20)
project(cascade LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

option(CASCADE_ENABLE_TSAN "Enable ThreadSanitizer" OFF)
option(CASCADE_ENABLE_ASAN "Enable AddressSanitizer" OFF)
option(CASCADE_ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)

if(CASCADE_ENABLE_TSAN)
    add_compile_options(-fsanitize=thread -g -O1)
    add_link_options(-fsanitize=thread)
endif()

if(CASCADE_ENABLE_ASAN)
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)
endif()

if(CASCADE_ENABLE_UBSAN)
    add_compile_options(-fsanitize=undefined)
    add_link_options(-fsanitize=undefined)
endif()

if(CASCADE_ENABLE_ASAN OR CASCADE_ENABLE_UBSAN)
    add_compile_options(-g -O1)
endif()

add_compile_options(-Wall -Wextra -Wpedantic)

include(FetchContent)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_subdirectory(cpp)
add_subdirectory(tests)
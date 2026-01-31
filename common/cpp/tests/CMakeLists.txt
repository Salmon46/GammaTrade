cmake_minimum_required(VERSION 3.16)
project(metrics_tests VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(prometheus-cpp REQUIRED)

include_directories(
    ${CMAKE_SOURCE_DIR}/..
)

add_executable(test_metrics test_metrics.cpp)

target_link_libraries(test_metrics
    prometheus-cpp::core
    pthread
    z
)

cmake_minimum_required(VERSION 3.16)
project(execution_router VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# =============================================================================
# Find Dependencies
# =============================================================================

# Protocol Buffers
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

# hiredis (Redis client)
find_package(PkgConfig REQUIRED)
pkg_check_modules(HIREDIS REQUIRED hiredis)

# spdlog
find_package(spdlog QUIET)
if(NOT spdlog_FOUND)
    pkg_check_modules(SPDLOG REQUIRED spdlog)
endif()

# nlohmann_json (for legacy compatibility)
find_package(nlohmann_json QUIET)

# Prometheus-cpp
find_package(prometheus-cpp REQUIRED)

# =============================================================================
# Compile Protocol Buffers
# =============================================================================

set(PROTO_DIR "${CMAKE_SOURCE_DIR}/common")
set(PROTO_FILES
    ${PROTO_DIR}/market_data.proto
    ${PROTO_DIR}/trade_signal.proto
    ${PROTO_DIR}/external_data.proto
    ${PROTO_DIR}/execution_report.proto
)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

# =============================================================================
# Include Directories
# =============================================================================

include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/common/cpp
    ${HIREDIS_INCLUDE_DIRS}
)

# =============================================================================
# Source Files
# =============================================================================

set(SOURCES
    src/main.cpp
    src/ExecutionRouter.cpp
    src/RedisClient.cpp
    ${PROTO_SRCS}
)

# =============================================================================
# Build Executable
# =============================================================================

add_executable(execution_router ${SOURCES})

target_link_libraries(execution_router
    ${Protobuf_LIBRARIES}
    ${HIREDIS_LIBRARIES}
    spdlog::spdlog
    prometheus-cpp::pull
    prometheus-cpp::core
    z
    curl
    pthread
)

# =============================================================================
# Installation
# =============================================================================

install(TARGETS execution_router DESTINATION bin)

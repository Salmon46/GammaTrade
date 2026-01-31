cmake_minimum_required(VERSION 3.16)
project(e_adapter_2_coinbase VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_compile_definitions(CPPHTTPLIB_OPENSSL_SUPPORT)

# =============================================================================
# Find Dependencies
# =============================================================================

# Protocol Buffers
find_package(Protobuf REQUIRED)
include_directories(${Protobuf_INCLUDE_DIRS})

# Boost (for ASIO used by websocketpp)
find_package(Boost REQUIRED COMPONENTS system thread)
include_directories(${Boost_INCLUDE_DIRS})

# OpenSSL
find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})

# hiredis (Redis client)
find_package(PkgConfig REQUIRED)
pkg_check_modules(HIREDIS REQUIRED hiredis)

# spdlog
find_package(spdlog QUIET)
if(NOT spdlog_FOUND)
    pkg_check_modules(SPDLOG REQUIRED spdlog)
endif()

# nlohmann_json
find_package(nlohmann_json QUIET)
if(NOT nlohmann_json_FOUND)
    message(STATUS "Using system nlohmann_json")
endif()

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
    src/CoinbaseExecutionAdapter.cpp
    src/RedisPublisher.cpp
    src/RedisSubscriber.cpp
    ${PROTO_SRCS}
)

# =============================================================================
# Build Executable
# =============================================================================

add_executable(e_adapter_2 ${SOURCES})

target_link_libraries(e_adapter_2
    ${Protobuf_LIBRARIES}
    ${Boost_LIBRARIES}
    ${OPENSSL_LIBRARIES}
    ${HIREDIS_LIBRARIES}
    spdlog::spdlog
    pthread
)

# Explicitly include /usr/local/include for httplib.h
target_include_directories(e_adapter_2 PRIVATE /usr/local/include)

# =============================================================================
# Installation
# =============================================================================

install(TARGETS e_adapter_2 DESTINATION bin)

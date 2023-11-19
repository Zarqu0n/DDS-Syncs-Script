cmake_minimum_required(VERSION 3.12.4)

if(NOT CMAKE_VERSION VERSION_LESS 3.0)
    cmake_policy(SET CMP0048 NEW)
endif()

project(DDSHelloWorld)

# Find requirements
if(NOT fastcdr_FOUND)
    find_package(fastcdr REQUIRED)
endif()

if(NOT fastrtps_FOUND)
    find_package(fastrtps REQUIRED)
endif()

# Set C++11
include(CheckCXXCompilerFlag)
if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_COMPILER_IS_CLANG OR
        CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    check_cxx_compiler_flag(-std=c++17 SUPPORTS_CXX17)
    if(SUPPORTS_CXX17)
        add_compile_options(-std=c++17)
    else()
        message(FATAL_ERROR "Compiler doesn't support C++17")
    endif()
endif()
add_compile_options(-I/usr/include/python3.8  -lpython3.8 -lpthread -ljsoncpp)

message(STATUS "Configuring HelloWorld publisher/subscriber example...")
file(GLOB DDS_HELLOWORLD_SOURCES_CXX "src/msg/*.cxx")
add_executable(Main.exe src/main.cpp ${DDS_HELLOWORLD_SOURCES_CXX})
target_link_libraries(Main.exe jsoncpp python3.8 pthread fastrtps fastcdr)
#target_link_libraries(DDSPublisher fastrtps fastcdr)

cmake_minimum_required(VERSION 3.24)
project(autobot_cpp LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
option(AUTOBOT_BUILD_EXECUTOR "Build gRPC live execution engine MVP" OFF)

add_subdirectory(src)

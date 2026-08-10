cmake_minimum_required(VERSION 3.20)
project(oms_algorithms LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(oms_algorithms oms_algorithms.cpp)
target_compile_options(oms_algorithms PRIVATE -Wall -Wextra -Wpedantic)
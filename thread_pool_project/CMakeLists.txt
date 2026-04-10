cmake_minimum_required(VERSION 3.10)
project(thread_pool_demo LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_executable(thread_pool_demo
    src/main.cpp
)

target_include_directories(thread_pool_demo PRIVATE include)
target_link_libraries(thread_pool_demo PRIVATE Threads::Threads)

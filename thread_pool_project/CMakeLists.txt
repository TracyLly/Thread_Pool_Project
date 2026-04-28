cmake_minimum_required(VERSION 3.10)

project(AdvancedThreadPoolProject LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

include_directories(${PROJECT_SOURCE_DIR}/include)

set(SOURCES
    src/main.cpp
    src/AdvancedThreadPool.cpp
    src/Logger.cpp
    src/Monitor.cpp
    src/SignalUtils.cpp
    src/SignalWatcher.cpp
    src/SimulatedWork.cpp
    src/TimeUtils.cpp
)

add_executable(advanced_thread_pool ${SOURCES})

target_link_libraries(advanced_thread_pool PRIVATE Threads::Threads)
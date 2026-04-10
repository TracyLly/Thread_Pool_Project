# Linux C++ Thread Pool Demo

## Build with g++
```bash
g++ -std=c++17 -O2 -Wall -Wextra -pthread -Iinclude src/main.cpp -o thread_pool_demo
./thread_pool_demo
```

## Build with CMake
```bash
mkdir -p build
cd build
cmake ..
make -j
./thread_pool_demo
```

## Project structure
- `include/ThreadPool.h`: thread pool implementation
- `src/main.cpp`: demo program
- `CMakeLists.txt`: CMake build script

## Features
- fixed-size worker threads
- task queue
- `std::mutex + std::condition_variable`
- `submit()` returns `std::future`
- graceful shutdown in destructor

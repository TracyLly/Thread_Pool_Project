CXX = g++
CXXFLAGS = -Iinclude -std=c++17 -Wall -pthread

TARGET = thread_pool
SRC = src/main.cpp

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(SRC) $(CXXFLAGS) -o $(TARGET)

clean:
	rm -f $(TARGET)

.PHONY: all clean
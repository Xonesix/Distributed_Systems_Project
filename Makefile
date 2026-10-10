CXX = /usr/bin/g++
CXXFLAGS = -std=c++17 -Wall

all: main binary

main: main.cpp
	$(CXX) $(CXXFLAGS) main.cpp -o main

binary: node.cpp
	$(CXX) $(CXXFLAGS) -pthread node.cpp -o binary

clean:
	rm -f main binary

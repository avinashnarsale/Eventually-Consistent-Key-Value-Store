# See README.txt.

.PHONY: all cpp clean

all: cpp 

cpp:    server   client 

clean:
	rm -f server client 
	rm -f protoc_middleman map.pb.cc map.pb.h map_pb2.py com/example/tutorial/mapProtos.java

server: server.cc
	pkg-config --cflags protobuf  # fails if protobuf is not installed
	c++ server.cc map.pb.cc -o server `pkg-config --cflags --libs protobuf` -std=c++11

client: client.cc
	pkg-config --cflags protobuf  # fails if protobuf is not installed
	c++ client.cc map.pb.cc -o client `pkg-config --cflags --libs protobuf` -std=c++11

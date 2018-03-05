# Eventually-Consistent-Key-Value-Store

Implemented distributed application of an eventually consistent key-value store based on designs from Dynamo and Cassandra using C++ and Protocol Buffers (Google ProtoBuf) along with TCP Socket for communication between different entities in the system. Operations allowed in key value store are:
get key – given a key, return its corresponding value
put key value – if the key does not already exist, create a new key-value pair; otherwise, update the key to the new value. 
Store accepts consistency level as an input as a depth of consistency and perform either of two operations (Read repair and Hinted handoff) to maintain store consistent. 

Compilation and execution: 

1. To compile code: Extract all files and folders to a directory. 
2. Set PATH and PKG_CONFIG_PATH to generate cpp code from bank.proto with "protoc --cpp_out=./ map.proto". 
	export PATH=/home/phao3/protobuf/bin/bin:$PATH
	export PKG_CONFIG_PATH=/home/phao3/protobuf/bin/lib/pkgconfig
3. This should generate .pb files. Run "make". 
4. Create list_of_replicas.txt file to store all replica server details
5. Once binary file has been created run branch with bash script as "bash server.sh <port#> <list_of_replicas.txt>" 
	Or run on command prompt "./server <port#> <list_of_replicas.txt>" with proper variables
6. Run client with bash script as "bash client.sh <port#> <list_of_replicas.txt>"
	Or run on command prompt "./client <port#> <list_of_replicas.txt>" with proper variables
7. Perform required operations.

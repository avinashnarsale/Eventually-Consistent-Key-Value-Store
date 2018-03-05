# Eventually-Consistent-Key-Value-Store

Implemented distributed application of an eventually consistent key-value store based on designs from Dynamo and Cassandra using C++ and Protocol Buffers (Google ProtoBuf) along with TCP Socket for communication between different entities in the system. Operations allowed in key value store are:
get key – given a key, return its corresponding value
put key value – if the key does not already exist, create a new key-value pair; otherwise, update the key to the new value. 
Store accepts consistency level as an input as a depth of consistency and perform either of two operations (Read repair and Hinted handoff) to maintain store consistent. 

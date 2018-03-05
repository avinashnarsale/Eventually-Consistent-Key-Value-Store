#!/bin/bash +vx
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:"/home/yaoliu/src_code/local/lib"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:"/home/phao3/protobuf/bin/lib"
./server $1 $2

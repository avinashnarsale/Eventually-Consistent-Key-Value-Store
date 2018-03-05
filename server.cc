#include <iostream>
#include <fstream>
#include <string>
#include <mutex>
#include <stdio.h>
#include <sys/types.h>
#include <pthread.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <netdb.h>
#include <sstream>
#include <algorithm>  
#include <ifaddrs.h>
#include <time.h>
#include <ctime>
#include <sys/time.h>
#include <list>
#include <thread>
#include <chrono> 
#include <random>
#include "map.pb.h"

using namespace std;

bool switch_HintedHandoff = false;
bool switch_ReadRepair = false;

std::mutex myMutex;

int local_port=0;
string local_ip;
int replication_factor = 4;
string commitLogFile;
string hintLogFile;

// stores all other servers(Port,IP) and key ranges(low,high) for each server
map<pair<int,string>,pair<int,int>> server_list;

// key with pairs of value and timestamp
map<int,pair<string,string>> mapStore;

// return value for replica read
ReadReplyFromReplica replica_return_data;

// read repair data structure 
typedef pair<int,pair<string,string>> serverValuesType;// key & <value,timestamp>
typedef pair<pair<int,string>,pair<int,int>> clientCLType;//client <Port,IP> & <required CL, current CL>
typedef map<pair<int,string>,serverValuesType> replicaServerReply;// multiple replica servers <Port,IP> & key, <value,timestamp>
int key_readRepair = 1; // token key for read operation
map <int,pair<clientCLType,replicaServerReply>> readRepair; // read repair data structure
map <int,int> isReplied; // check for replied status 0:entry, 1:CL_OK & 2:Exception


// Hinted handoff data structure 
//typedef pair<pair<int,string>,pair<int,int>> clientCLType;//client <Port,IP> & <required CL, current CL>
typedef map<pair<int,string>,string> replicaServerWriteReply;// multiple replica servers <Port,IP> & ACK/NACK
int key_hintedHandoff = 1; // token key for write operation
map <int,pair<clientCLType,replicaServerWriteReply>> hintedHandoff; // hinted handoff data structure
map <int,int> isRepliedWrite; // check for replied status 0:entry, 1:CL_OK & 2:Exception

bool entryToCommitLog(WriteToReplica entryData){
	CommitLog commit_log;
	{
		fstream input_log(commitLogFile.c_str(), ios::in | ios::binary);
		if (!input_log) {
		  cout << commitLogFile << ": Log file not found.. Creating a new log file." << endl;
		} else if (!commit_log.ParseFromIstream(&input_log)) {
		  cerr << "Failed to parse log file." << endl;
		  return false;
		}
	}

	bool alreadyExist=false;
	for (int i = 0; i < commit_log.commit_log_size(); i++) {
		if(entryData.key()==commit_log.mutable_commit_log(i)->key()){
			commit_log.mutable_commit_log(i)->set_value(entryData.value());
			commit_log.mutable_commit_log(i)->set_timestamp(entryData.timestamp());
			alreadyExist=true;
		}
	}
	
	if(!alreadyExist){
		WriteToReplica *logData=commit_log.add_commit_log();
		logData->set_key(entryData.key());
		logData->set_value(entryData.value());
		logData->set_timestamp(entryData.timestamp());
	}

	{
		fstream  write_log(commitLogFile.c_str(), ios::out | ios::trunc | ios::binary);
		if (!commit_log.SerializeToOstream(&write_log)) {
		  cerr << "Failed to write commit log." << endl;
		  return false;
		}
	}
	return true;
}

bool entryToHintLog(WriteToReplica entryData, int from_port, string from_ip){
	HintedHandoffLog hint_log;
	{
		fstream input_log(hintLogFile.c_str(), ios::in | ios::binary);
		if (!input_log) {
		  cout << hintLogFile << ": Log file not found.. Creating a new log file." << endl;
		} else if (!hint_log.ParseFromIstream(&input_log)) {
		  cerr << "Failed to parse log file." << endl;
		  return false;
		}
	}

	{
		HintedHandoff *newEntry=hint_log.add_hinted_log();
		newEntry->set_key(entryData.key());
		newEntry->set_value(entryData.value());
		newEntry->set_timestamp(entryData.timestamp());
		newEntry->set_for_server_port(from_port);
		newEntry->set_for_server_ip(from_ip);
	}

	{
		fstream  write_log(hintLogFile.c_str(), ios::out | ios::trunc | ios::binary);
		if (!hint_log.SerializeToOstream(&write_log)) {
		  cerr << "Failed to write commit log." << endl;
		  return false;
		}
	}
	return true;
}

void update_mapStore(int key, pair<string,string> values){
	auto itr=mapStore.find(key);
	if(itr!=mapStore.end()){
		if(itr->second.second < values.second){
			itr->second=values;
		}else{
			cout << local_port << "Already holds latest value for:" << key << endl;
		}
	}else{
		mapStore.insert(make_pair(key,values));
	}
}

void read_repair_and_check(){
	cout << local_port << "::" << "Read repart thread started. " << endl;
	while(1){
		bool needRepair=false;
		// return value for client read
		ReturnData return_data;

		for(auto itr_RR1=readRepair.begin();itr_RR1 != readRepair.end();++itr_RR1){
			auto itr_isR_1 = isReplied.find(itr_RR1->first);
			if(itr_isR_1!=isReplied.end()){
				//cout << local_port << "::" << "already replied for this key_readRepair:" << itr_RR1->first << endl;
			}else{
				if(itr_RR1->second.first.second.second==4){//itr_RR1->second.first.second.first){
					isReplied.insert(make_pair(itr_RR1->first,0));
					cout << local_port << "::" << "Request CL:" << itr_RR1->second.first.second.first << 
												"Read CL:" << itr_RR1->second.first.second.second << " satisfied!!" << endl;
					int conLevel=itr_RR1->second.first.second.first;
					bool isFirstWrite=true;
					for(auto iPrt = itr_RR1->second.second.begin();iPrt != itr_RR1->second.second.end();++iPrt){
						if(iPrt->second.first!=999){
							if(conLevel==0){break;}
							conLevel--;
							cout << local_port << "::" << "From replica server port:" << iPrt->first.first << 
										" IP:" << iPrt->first.second << endl;
							cout << local_port << "::" << "Key: " << iPrt->second.first;
							cout << " Value: " << iPrt->second.second.first;
							cout << " Timestamp: " << iPrt->second.second.second << endl;
							if(isFirstWrite){
								isFirstWrite=false;
								return_data.set_key(iPrt->second.first);
								return_data.set_value(iPrt->second.second.first);
								return_data.set_timestamp(iPrt->second.second.second);
							}else{
								if(iPrt->second.second.second>return_data.timestamp()){
									needRepair=true;
									return_data.set_key(iPrt->second.first);
									return_data.set_value(iPrt->second.second.first);
									return_data.set_timestamp(iPrt->second.second.second);
								}
								if(iPrt->second.second.second<return_data.timestamp()){
									needRepair=true;
								}
							}
						}
						else{
							needRepair=true;
						}
					}
					std::this_thread::sleep_for(std::chrono::milliseconds(500));
					{
						cout << local_port << "::" << "Check Point ------------->:: 1.1 :: " << conLevel << endl;
						string return_client_str;
						MapMessage return_client;
						if(conLevel==0){
							return_client.set_allocated_map_metadata(&return_data);
							if (!return_client.SerializeToString(&return_client_str)) {
								cerr << "ERROR: Failed to return_client message." << endl;
								exit(EXIT_FAILURE);
							}
							return_client.release_map_metadata();
						}else{
							return_client_str="Read, failed with consistency level criteria.";//"NACK";
						}
						struct sockaddr_in address;
						int sock = 0, valread;
						struct sockaddr_in serv_addr;
						char buffer[1024] = {0};
						memset(&serv_addr, '0', sizeof(serv_addr));
						if ((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
							cerr << local_port << "::" << "1.0 ERROR: Socket creation error." << endl;
							exit(EXIT_FAILURE);
						}
						serv_addr.sin_family = AF_INET;
						serv_addr.sin_port = htons(itr_RR1->second.first.first.first);
						if(inet_pton(AF_INET, itr_RR1->second.first.first.second.c_str(), &serv_addr.sin_addr)<=0) {
							cerr << local_port << "::" << "1.0.1 ERROR: Invalid address/ Address not supported." << endl;
							exit(EXIT_FAILURE);
						}
						if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
							cout << local_port << "::" << "1.1 ERROR: Read Repair, Client not available. " << endl;
							//exit(EXIT_FAILURE);
						}else{
							if(send(sock , return_client_str.c_str() , return_client_str.size() , 0 ) != return_client_str.size()){
								cerr << local_port << "::" << "1.2 ERROR: In sending back to Client." << endl;
								exit(EXIT_FAILURE);
							}
						}
						//std::this_thread::sleep_for(std::chrono::milliseconds(500));
						close(sock);
					}
					if(needRepair && conLevel==0 && switch_ReadRepair==true){
						cout << local_port << "::" << " Repairing needed, Starting... " << endl;
						ReadRepairWrite repair_data;
						repair_data.set_key(return_data.key());
						repair_data.set_value(return_data.value());
						repair_data.set_timestamp(return_data.timestamp());
						MapMessage repair_others;
						string repair_str;
						repair_others.set_allocated_map_read_repair_write(&repair_data);
						if (!repair_others.SerializeToString(&repair_str)) {
							cerr << local_port << "::" << "1.3 ERROR: Failed to repair_others message." << endl;
							exit(EXIT_FAILURE);
						}
						repair_others.release_map_read_repair_write();
						for(auto iPrt = itr_RR1->second.second.begin();iPrt != itr_RR1->second.second.end();++iPrt){
							if(iPrt->first.first==local_port&&iPrt->first.second==local_ip){
								myMutex.lock();
								update_mapStore(repair_data.key(),make_pair(repair_data.value(),repair_data.timestamp()));
								myMutex.unlock();
							}else{
								struct sockaddr_in address;
								int sock = 0, valread;
								struct sockaddr_in serv_addr;
								char buffer[1024] = {0};
								memset(&serv_addr, '0', sizeof(serv_addr));
								if ((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
									cerr << local_port << "::" << "1.4 ERROR: Socket creation error." << endl;
									exit(EXIT_FAILURE);
								}
								serv_addr.sin_family = AF_INET;
								serv_addr.sin_port = htons(iPrt->first.first);
								if(inet_pton(AF_INET, iPrt->first.second.c_str(), &serv_addr.sin_addr)<=0) {
									cerr << local_port << "::" << "1.5 ERROR: Invalid address/address not supported." << endl;
									exit(EXIT_FAILURE);
								}
								if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
									cout << local_port << "::" << " Repair not popssible on PORT:" << iPrt->first.first
																<< " IP:" << iPrt->first.second << endl;
								}else{
									if(send(sock , repair_str.c_str() , repair_str.size() , 0 ) != repair_str.size()){
										cerr << local_port << "::" << "1.6 ERROR: In sending, repair_str." << endl;
										exit(EXIT_FAILURE);
									}
								}
								close(sock);
							}
						}
					}
				}
			}
		}
		
		// Write ack/nack
		for(auto itr_WW1=hintedHandoff.begin();itr_WW1 != hintedHandoff.end();++itr_WW1){
			auto itr_isR_1 = isRepliedWrite.find(itr_WW1->first);
			if(itr_isR_1!=isRepliedWrite.end()){
				//cout << local_port << "::" << "already replied for this key_readRepair:" << itr_WW1->first << endl;
			}else{
				if(itr_WW1->second.first.second.second==4){//itr_WW1->second.first.second.first){
					isRepliedWrite.insert(make_pair(itr_WW1->first,0));
					cout << local_port << "::" << "Write Request CL:" << itr_WW1->second.first.second.first << 
												"Write CL:" << itr_WW1->second.first.second.second << " satisfied!!" << endl;
					int conLevelWrt=itr_WW1->second.first.second.first;
					for(auto iPrt = itr_WW1->second.second.begin();iPrt != itr_WW1->second.second.end();++iPrt){
						if(iPrt->second=="ACK"){
							if(conLevelWrt==0){break;}
							conLevelWrt--;
							cout << local_port << "::" << "Write from replica server port:" << iPrt->first.first << 
										" IP:" << iPrt->first.second << endl;
							cout << local_port << "::" << "ACK/NACK: " << iPrt->second << endl;
						}
					}
					std::this_thread::sleep_for(std::chrono::milliseconds(500));
					{
						cout << local_port << "::" << "Write Check Point ------------->:: 3 :: " << conLevelWrt << endl;
						string ack_client_str;
						if(conLevelWrt==0){
							ack_client_str="Write done successfully!";//"ACK";
						}else{
							ack_client_str="Write, failed with consistency level criteria.";//"NACK";
						}
						struct sockaddr_in address;
						int sock = 0, valread;
						struct sockaddr_in serv_addr;
						char buffer[1024] = {0};
						memset(&serv_addr, '0', sizeof(serv_addr));
						if ((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
							cerr << "ERROR: Socket creation error." << endl;
							exit(EXIT_FAILURE);
						}
						serv_addr.sin_family = AF_INET;
						serv_addr.sin_port = htons(itr_WW1->second.first.first.first);
						if(inet_pton(AF_INET, itr_WW1->second.first.first.second.c_str(), &serv_addr.sin_addr)<=0) {
							cerr << "ERROR: Invalid address/ Address not supported." << endl;
							exit(EXIT_FAILURE);
						}
						if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
							cout << local_port << "::" << "ERROR:: Write acknowledgment failed, Client not available. " << endl;
						}else{
							if(send(sock , ack_client_str.c_str(), ack_client_str.size(), 0 ) != ack_client_str.size()){
								cerr << "1.0 ERROR: In sending back socket_buffer." << endl;
								exit(EXIT_FAILURE);
							}
						}
						//std::this_thread::sleep_for(std::chrono::milliseconds(500));
						close(sock);
					}
				}
			}
		}
	}//while(1)
}

int writeToStore(MapStoreWrite inData, int in_key_hintedHandoff){
	if(inData.key()<0 || inData.key() > 255){
		cerr << "ERROR: Key value " << inData.key() << " is out of range" << endl;
		return -1;
	}
	
	struct timeval tv;
	char time_buffer[40];
	time_t curtime;
	gettimeofday(&tv, NULL); 
	curtime=tv.tv_sec;
	strftime(time_buffer,30,"%m-%d-%Y  %T.",localtime(&curtime));
	std::string current_timestamp(time_buffer);
	current_timestamp += std::to_string(tv.tv_usec);
	
	WriteToReplica repW_data;
	repW_data.set_key(inData.key());
	repW_data.set_value(inData.value());
	repW_data.set_timestamp(current_timestamp);
	repW_data.set_co_port(local_port);
	repW_data.set_co_ip(local_ip);
	repW_data.set_client_port(inData.client_port());
	repW_data.set_client_ip(inData.client_ip());
	repW_data.set_key_hinted_handoff(in_key_hintedHandoff);
	repW_data.set_write_level(inData.write_level());
	
	string output;
	MapMessage repW_map_message;
	repW_map_message.set_allocated_map_replica_write(&repW_data);
	if (!repW_map_message.SerializeToString(&output)) {
		cerr << "ERROR: Failed to write repW_data message." << endl;
		exit(EXIT_FAILURE);
	}
	repW_map_message.release_map_replica_write();
	
	bool isWithinRange=false;
	replication_factor=4; // Reset replication factor!!
	string ack_string = "";

	auto itr3=server_list.begin();
 	while(replication_factor!=0){
		if(isWithinRange){
			if(itr3->first.first==local_port && itr3->first.second==local_ip){
				cout << local_port << "::" << "SecRep Co-Ordinator: Adding/Updating entry for key:" << inData.key() << endl;
				bool isLogEntryDone = entryToCommitLog(repW_data);
				myMutex.lock();
				update_mapStore(inData.key(),make_pair(inData.value(),current_timestamp));
				myMutex.unlock();
				auto itr_W1 = hintedHandoff.find(in_key_hintedHandoff);
				if(itr_W1 != hintedHandoff.end()){
					//cout << local_port << "::" << "check point 6.1 Update for in_key_hintedHandoff::::::" << in_key_hintedHandoff << endl;
					itr_W1->second.first.second.second = itr_W1->second.first.second.second+1;
					replicaServerWriteReply temp_map=itr_W1->second.second;
					temp_map.insert(make_pair(make_pair(local_port,local_ip),"ACK"));
					itr_W1->second.second=temp_map;
				}else{
					//cout << local_port << "::" << "check point 6.2 Entry for in_key_hintedHandoff::::::" << in_key_hintedHandoff << endl;
					replicaServerWriteReply temp_map;
					temp_map.insert(make_pair(make_pair(local_port,local_ip),"ACK"));
					hintedHandoff.insert(make_pair(in_key_hintedHandoff,
							make_pair(make_pair(make_pair(inData.client_port(),inData.client_ip()),make_pair(inData.write_level(),1)),
							temp_map)));
				}
			}else{
				cout << local_port << "::" << "SecRep rep_server: Adding/Updating entry for key:" << inData.key() << endl;
				struct sockaddr_in address;
				int sock = 0, valread;
				struct sockaddr_in serv_addr;
				char buffer[1024] = {0};
				memset(&serv_addr, '0', sizeof(serv_addr));
				if ((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
					cerr << "ERROR: Socket creation error." << endl;
					exit(EXIT_FAILURE);
				}
				serv_addr.sin_family = AF_INET;
				serv_addr.sin_port = htons(itr3->first.first);
				if(inet_pton(AF_INET, itr3->first.second.c_str(), &serv_addr.sin_addr)<=0) {
					cerr << "ERROR: Invalid address/ Address not supported." << endl;
					exit(EXIT_FAILURE);
				}
				if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
					cout << local_port << "::" << "W1 Replica server not available, storing HINT.." << itr3->first.first << endl;
					bool isHintEntryDone = entryToHintLog(repW_data,itr3->first.first,itr3->first.second);
					auto itr_W1 = hintedHandoff.find(in_key_hintedHandoff);
					if(itr_W1 != hintedHandoff.end()){
						itr_W1->second.first.second.second = itr_W1->second.first.second.second+1;
						replicaServerWriteReply temp_map=itr_W1->second.second;
						temp_map.insert(make_pair(make_pair(itr3->first.first,itr3->first.second),"NACK"));
						itr_W1->second.second=temp_map;
					}else{
						replicaServerWriteReply temp_map;
						temp_map.insert(make_pair(make_pair(itr3->first.first,itr3->first.second),"NACK"));
						hintedHandoff.insert(make_pair(in_key_hintedHandoff,
								make_pair(make_pair(make_pair(inData.client_port(),inData.client_ip()),make_pair(inData.write_level(),1)),
								temp_map)));
					}
				}else{
					send(sock, output.c_str() , output.size(), 0);
					close(sock);
				}
			}
			replication_factor--;
		}
		if(inData.key()>=itr3->second.first && inData.key() <= itr3->second.second){
			if(itr3->first.first==local_port && itr3->first.second==local_ip){
				cout << local_port << "::" << "MainRep Co-Ordinator: Adding/Updating entry for key:" << inData.key() << endl;
				bool isLogEntryDone = entryToCommitLog(repW_data);
				myMutex.lock();
				update_mapStore(inData.key(),make_pair(inData.value(),current_timestamp));
				myMutex.unlock();
				auto itr_W1 = hintedHandoff.find(in_key_hintedHandoff);
				if(itr_W1 != hintedHandoff.end()){
					//cout << local_port << "::" << "check point 6.1 Update for in_key_hintedHandoff::::::" << in_key_hintedHandoff << endl;
					itr_W1->second.first.second.second = itr_W1->second.first.second.second+1;
					replicaServerWriteReply temp_map=itr_W1->second.second;
					temp_map.insert(make_pair(make_pair(local_port,local_ip),"ACK"));
					itr_W1->second.second=temp_map;
				}else{
					//cout << local_port << "::" << "check point 6.2 Entry for in_key_hintedHandoff::::::" << in_key_hintedHandoff << endl;
					replicaServerWriteReply temp_map;
					temp_map.insert(make_pair(make_pair(local_port,local_ip),"ACK"));
					hintedHandoff.insert(make_pair(in_key_hintedHandoff,
							make_pair(make_pair(make_pair(inData.client_port(),inData.client_ip()),make_pair(inData.write_level(),1)),
							temp_map)));
				}
			}else{
				cout << local_port << "::" << "MainRep rep_server: Adding/Updating entry for key:" << inData.key() << endl;
				struct sockaddr_in address;
				int sock = 0, valread;
				struct sockaddr_in serv_addr;
				char buffer[1024] = {0};
				memset(&serv_addr, '0', sizeof(serv_addr));
				if ((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
					cerr << "ERROR: Socket creation error." << endl;
					exit(EXIT_FAILURE);
				}
				serv_addr.sin_family = AF_INET;
				serv_addr.sin_port = htons(itr3->first.first);
				if(inet_pton(AF_INET, itr3->first.second.c_str(), &serv_addr.sin_addr)<=0) {
					cerr << "ERROR: Invalid address/ Address not supported." << endl;
					exit(EXIT_FAILURE);
				}
				if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
					cout << local_port << "::" << "W2 Replica server not available, storing HINT.." << itr3->first.first << endl;
					bool isHintEntryDone = entryToHintLog(repW_data,itr3->first.first,itr3->first.second);
					auto itr_W1 = hintedHandoff.find(in_key_hintedHandoff);
					if(itr_W1 != hintedHandoff.end()){
						itr_W1->second.first.second.second = itr_W1->second.first.second.second+1;
						replicaServerWriteReply temp_map=itr_W1->second.second;
						temp_map.insert(make_pair(make_pair(itr3->first.first,itr3->first.second),"NACK"));
						itr_W1->second.second=temp_map;
					}else{
						replicaServerWriteReply temp_map;
						temp_map.insert(make_pair(make_pair(itr3->first.first,itr3->first.second),"NACK"));
						hintedHandoff.insert(make_pair(in_key_hintedHandoff,
								make_pair(make_pair(make_pair(inData.client_port(),inData.client_ip()),make_pair(inData.write_level(),1)),
								temp_map)));
					}
				}else{
					send(sock, output.c_str() , output.size(), 0);
					close(sock);
				}
			}
			isWithinRange=true;
			replication_factor--;
		}
		++itr3;
		if(itr3==server_list.end()){
			itr3=server_list.begin();
		}
	}
	return 0;
}

int replicaWrite(WriteToReplica inData){
	bool isLogEntryDone = entryToCommitLog(inData);
	myMutex.lock();
	update_mapStore(inData.key(),make_pair(inData.value(),inData.timestamp()));
	myMutex.unlock();
}

int repair_replicaWrite(ReadRepairWrite inData){
	WriteToReplica temp_log;
	temp_log.set_key(inData.key());
	temp_log.set_value(inData.value());
	temp_log.set_timestamp(inData.timestamp());
	bool isLogEntryDone = entryToCommitLog(temp_log);
	myMutex.lock();
	update_mapStore(inData.key(),make_pair(inData.value(),inData.timestamp()));
	myMutex.unlock();
}

int handoff_replicaWrite(HintedHandoffWrite inData){
	WriteToReplica temp_log;
	temp_log.set_key(inData.key());
	temp_log.set_value(inData.value());
	temp_log.set_timestamp(inData.timestamp());
	bool isLogEntryDone = entryToCommitLog(temp_log);
	myMutex.lock();
	update_mapStore(inData.key(),make_pair(inData.value(),inData.timestamp()));
	myMutex.unlock();
}

void readFromStore(MapStoreRead inData,int in_key_readRepair){
	bool isWithinRange=false;
	replication_factor=4; // Reset replication factor!!
	string ack_string = "";
	
	ReadFromReplica repR_data;
	repR_data.set_key(inData.key());
	repR_data.set_key_read_repair(in_key_readRepair);
	repR_data.set_client_port(inData.client_port());
	repR_data.set_client_ip(inData.client_ip());
	repR_data.set_read_level(inData.read_level());
	repR_data.set_co_port(local_port);
	repR_data.set_co_ip(local_ip);
	
	string output;
	MapMessage repR_map_message;
	repR_map_message.set_allocated_map_replica_read(&repR_data);
	if (!repR_map_message.SerializeToString(&output)) {
		cerr << "ERROR: Failed to write repR_data message." << endl;
		exit(EXIT_FAILURE);
	}
	repR_map_message.release_map_replica_read();
	auto itr3=server_list.begin();
 	while(replication_factor!=0){
		if(isWithinRange){
			if(itr3->first.first==local_port && itr3->first.second==local_ip){
				cout << local_port << "::" << "SecRep Co-Ordinator: Reading for key:" << inData.key() << endl;
				auto itr1 = mapStore.find(inData.key());
				serverValuesType temp_StoreValue;
				if (itr1 != mapStore.end()){
					temp_StoreValue=make_pair(itr1->first,itr1->second);
				}else{
					temp_StoreValue=make_pair(999,make_pair("Co-OrdinatorNotFound","NoTimeStamp1"));
				}
				auto itr_R1 = readRepair.find(in_key_readRepair);
				if(itr_R1 != readRepair.end()){
					cout << local_port << "::" << "check point 4.1 Update for key_readRepair::::::" << in_key_readRepair << endl;
					itr_R1->second.first.second.second = itr_R1->second.first.second.second+1;
					replicaServerReply temp_map=itr_R1->second.second;
					temp_map.insert(make_pair(make_pair(local_port,local_ip),temp_StoreValue));
					itr_R1->second.second=temp_map;
				}else{
					cout << local_port << "::" << "check point 4.2 Entry for key_readRepair::::::" << in_key_readRepair << endl;
					replicaServerReply temp_map;
					temp_map.insert(make_pair(make_pair(local_port,local_ip),temp_StoreValue));
					readRepair.insert(make_pair(in_key_readRepair,
							make_pair(make_pair(make_pair(inData.client_port(),inData.client_ip()),make_pair(inData.read_level(),1)),
							temp_map)));
				}
			}else{
				cout << local_port << "::" << "SecRep rep_server: Reading for key:" << inData.key() << endl;
				struct sockaddr_in address;
				int sock = 0, valread;
				struct sockaddr_in serv_addr;
				char buffer[1024] = {0};
				memset(&serv_addr, '0', sizeof(serv_addr));
				if ((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
					cerr << "ERROR: Socket creation error." << endl;
					exit(EXIT_FAILURE);
				}
				serv_addr.sin_family = AF_INET;
				serv_addr.sin_port = htons(itr3->first.first);
				if(inet_pton(AF_INET, itr3->first.second.c_str(), &serv_addr.sin_addr)<=0) {
					cerr << "ERROR: Invalid address/ Address not supported." << endl;
					exit(EXIT_FAILURE);
				}
				if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
					cout << local_port << "::" << "R1 Replica server not available:" << itr3->first.first << endl;
					serverValuesType temp_StoreValue;
					temp_StoreValue=make_pair(999,make_pair("ReplicaDown","NoTimeStamp2"));
					auto itr_R1 = readRepair.find(in_key_readRepair);
					if(itr_R1 != readRepair.end()){
						itr_R1->second.first.second.second = itr_R1->second.first.second.second+1;
						replicaServerReply temp_map=itr_R1->second.second;
						temp_map.insert(make_pair(make_pair(itr3->first.first,itr3->first.second),temp_StoreValue));
						itr_R1->second.second=temp_map;
					}else{
						replicaServerReply temp_map;
						temp_map.insert(make_pair(make_pair(itr3->first.first,itr3->first.second),temp_StoreValue));
						readRepair.insert(make_pair(in_key_readRepair,
								make_pair(make_pair(make_pair(inData.client_port(),inData.client_ip()),make_pair(inData.read_level(),1)),
								temp_map)));
					}
				}else{
					send(sock, output.c_str() , output.size(), 0);
				}
				close(sock);
			}
			replication_factor--;
		}
		if(inData.key()>=itr3->second.first && inData.key() <= itr3->second.second){
			if(itr3->first.first==local_port && itr3->first.second==local_ip){
				cout << local_port << "::" << "MainRep Co-Ordinator: Reading for key:" << inData.key() << endl;
				auto itr1 = mapStore.find(inData.key());
				serverValuesType temp_StoreValue;
				if (itr1 != mapStore.end()){
					temp_StoreValue=make_pair(itr1->first,itr1->second);
				}else{
					temp_StoreValue=make_pair(999,make_pair("Co-OrdinatorNotFound","NoTimeStamp1"));
				}
				auto itr_R1 = readRepair.find(in_key_readRepair);
				if(itr_R1 != readRepair.end()){
					cout << local_port << "::" << "check point 4.3 Update for key_readRepair::::::" << in_key_readRepair << endl;
					itr_R1->second.first.second.second = itr_R1->second.first.second.second+1;
					replicaServerReply temp_map=itr_R1->second.second;
					temp_map.insert(make_pair(make_pair(local_port,local_ip),temp_StoreValue));
					itr_R1->second.second=temp_map;
				}else{
					cout << local_port << "::" << "check point 4.4 Entry for key_readRepair::::::" << in_key_readRepair << endl;
					replicaServerReply temp_map;
					temp_map.insert(make_pair(make_pair(local_port,local_ip),temp_StoreValue));
					readRepair.insert(make_pair(in_key_readRepair,
							make_pair(make_pair(make_pair(inData.client_port(),inData.client_ip()),make_pair(inData.read_level(),1)),
							temp_map)));
				}
			}else{
				cout << local_port << "::" << "MainRep rep_server: Reading for key:" << inData.key() << endl;
				struct sockaddr_in address;
				int sock = 0, valread;
				struct sockaddr_in serv_addr;
				char buffer[1024] = {0};
				memset(&serv_addr, '0', sizeof(serv_addr));
				if ((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
					cerr << "ERROR: Socket creation error." << endl;
					exit(EXIT_FAILURE);
				}
				serv_addr.sin_family = AF_INET;
				serv_addr.sin_port = htons(itr3->first.first);
				if(inet_pton(AF_INET, itr3->first.second.c_str(), &serv_addr.sin_addr)<=0) {
					cerr << "ERROR: Invalid address/ Address not supported." << endl;
					exit(EXIT_FAILURE);
				}
				if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
					cout << local_port << "::" << "R2 Replica server not available:" << itr3->first.first << endl;
					serverValuesType temp_StoreValue;
					temp_StoreValue=make_pair(999,make_pair("ReplicaDown","NoTimeStamp2"));
					auto itr_R1 = readRepair.find(in_key_readRepair);
					if(itr_R1 != readRepair.end()){
						itr_R1->second.first.second.second = itr_R1->second.first.second.second+1;
						replicaServerReply temp_map=itr_R1->second.second;
						temp_map.insert(make_pair(make_pair(itr3->first.first,itr3->first.second),temp_StoreValue));
						itr_R1->second.second=temp_map;
					}else{
						replicaServerReply temp_map;
						temp_map.insert(make_pair(make_pair(itr3->first.first,itr3->first.second),temp_StoreValue));
						readRepair.insert(make_pair(in_key_readRepair,
								make_pair(make_pair(make_pair(inData.client_port(),inData.client_ip()),make_pair(inData.read_level(),1)),
								temp_map)));
					}
				}else{
					send(sock, output.c_str() , output.size(), 0);
				}
				close(sock);
			}
			isWithinRange=true;
			replication_factor--;
		}
		++itr3;
		if(itr3==server_list.end()){
			itr3=server_list.begin();
		}
	}
}

bool replicaRead(ReadFromReplica inData){
	auto itr1 = mapStore.find(inData.key());
	if (itr1 != mapStore.end()){
		cout << local_port << "::" << "check point 2 replica read data found." << endl;
		replica_return_data.set_key(itr1->first);
		replica_return_data.set_value(itr1->second.first);
		replica_return_data.set_timestamp(itr1->second.second);
		replica_return_data.set_read_level(inData.read_level());
		replica_return_data.set_client_ip(inData.client_ip());
		replica_return_data.set_client_port(inData.client_port());
		replica_return_data.set_server_ip(local_ip);
		replica_return_data.set_server_port(local_port);
		replica_return_data.set_key_read_repair(inData.key_read_repair());
	}else{
		cout << local_port << "::" << "check point 3 replica read data NOT found." << endl;
		replica_return_data.set_key(999);
		replica_return_data.set_value("ReplicaNoData");
		replica_return_data.set_timestamp("NoTimeStamp3");
		replica_return_data.set_read_level(inData.read_level());
		replica_return_data.set_client_ip(inData.client_ip());
		replica_return_data.set_client_port(inData.client_port());
		replica_return_data.set_server_ip(local_ip);
		replica_return_data.set_server_port(local_port);
		replica_return_data.set_key_read_repair(inData.key_read_repair());
	}
	return true;
}

void hinted_HandoffCheck(int in_serverPort,string in_serverIP){
	HintedHandoffLog hint_log_read;
	HintedHandoffLog hint_log_write;
	{
		fstream input_log(hintLogFile.c_str(), ios::in | ios::binary);
		if (!input_log) {
		  cout << hintLogFile << ": Log file not found.. Creating a new log file." << endl;
		} else {
			if (!hint_log_read.ParseFromIstream(&input_log)) {
			  cerr << "Failed to parse log file." << endl;
			}else{
				for (int i = 0; i < hint_log_read.hinted_log_size(); i++) {
					HintedHandoff t_data=hint_log_read.hinted_log(i);
					if(t_data.for_server_port()==in_serverPort && t_data.for_server_ip()==in_serverIP){
						// send handoff
						cout << local_port << "::" << "Sending Handoff copy..." << endl;
						cout << local_port << "::" << "To port:" << t_data.for_server_port();
						cout << " IP:" << t_data.for_server_ip();
						cout << " key:" << t_data.key();
						cout << " value:" << t_data.value();
						cout << " TS:" << t_data.timestamp() << endl;
						HintedHandoffWrite handoff_data;
						handoff_data.set_key(t_data.key());
						handoff_data.set_value(t_data.value());
						handoff_data.set_timestamp(t_data.timestamp());
						MapMessage handoff_co;
						string handoff_str;
						handoff_co.set_allocated_map_handoff_replica_write(&handoff_data);
						if (!handoff_co.SerializeToString(&handoff_str)) {
							cerr << "ERROR: Failed to handoff_co message." << endl;
							exit(EXIT_FAILURE);
						}
						handoff_co.release_map_handoff_replica_write();
						struct sockaddr_in address;
						int sock = 0, valread;
						struct sockaddr_in serv_addr;
						char buffer[1024] = {0};
						memset(&serv_addr, '0', sizeof(serv_addr));
						if ((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
							cerr << "ERROR: Socket creation error." << endl;
							exit(EXIT_FAILURE);
						}
						serv_addr.sin_family = AF_INET;
						serv_addr.sin_port = htons(t_data.for_server_port());
						if(inet_pton(AF_INET, t_data.for_server_ip().c_str(), &serv_addr.sin_addr)<=0) {
							cerr << "ERROR: Invalid address/ Address not supported." << endl;
							exit(EXIT_FAILURE);
						}
						if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
							cout << local_port << "::" << "Handoff not popssible on port:" << t_data.for_server_port()
														<< " IP:" << t_data.for_server_ip() << endl;
						}else{
							if(send(sock, handoff_str.c_str(), handoff_str.size(), 0 ) != handoff_str.size()){
								cerr << "1.0 ERROR: In sending back socket_buffer." << endl;
								exit(EXIT_FAILURE);
							}
						}
						close(sock);
						///////////
					}else{
						HintedHandoff *t_add_data=hint_log_write.add_hinted_log();
						t_add_data->set_key(t_data.key());
						t_add_data->set_value(t_data.value());
						t_add_data->set_timestamp(t_data.timestamp());
						t_add_data->set_for_server_port(t_data.for_server_port());
						t_add_data->set_for_server_ip(t_data.for_server_ip());
					}
				}
			}
		}
	}
	{
		fstream  write_log(hintLogFile.c_str(), ios::out | ios::trunc | ios::binary);
		if (!hint_log_write.SerializeToOstream(&write_log)) {
		  cerr << "Failed to re-write hintedHandoff log." << endl;
		}
	}

}

int main(int argc, char* argv[]) { 
	GOOGLE_PROTOBUF_VERIFY_VERSION;

	if (argc != 3) {
		cerr << "Usage: " << argv[0] << " ./server <port_number> <server_list_file>" << endl;
		return -1;
	}
	int select_Consistency;
	int on_readRep;
	cout << "Select consistency 1:Hinted Handoff,2:Read Repair,3:Both):";
	cin >> select_Consistency;
	if(select_Consistency==1){
		switch_HintedHandoff=true;
	}
	if(select_Consistency==2){
		switch_ReadRepair=true;
	}
	if(select_Consistency==3){
		switch_HintedHandoff=true;
		switch_ReadRepair=true;
	}

	commitLogFile="CommitLog_";
	commitLogFile += argv[1];
	hintLogFile="HintLog_";
	hintLogFile += argv[1];
	// Load data from log file
	CommitLog commit_log;
	{
		fstream input_log(commitLogFile.c_str(), ios::in | ios::binary);
		if (!input_log) {
		  cout << commitLogFile << ": Log file not found.. Creating a new log file." << endl;
		} else {
			if (!commit_log.ParseFromIstream(&input_log)) {
			  cerr << "Failed to parse log file." << endl;
			}else{
				for (int i = 0; i < commit_log.commit_log_size(); i++) {
					WriteToReplica t_data=commit_log.commit_log(i);
					myMutex.lock();
					update_mapStore(t_data.key(),make_pair(t_data.value(),t_data.timestamp()));
					myMutex.unlock();
				}
			}
		}
	}
	
	ifstream infile(argv[2]);
	string line;
	int number_of_servers=0;
	while (getline(infile, line)){
		number_of_servers++;
	}
	int range=255/number_of_servers;
	int increment_range=0;
	infile.clear();
	infile.seekg(0, ios::beg);
	while (getline(infile, line)){
		istringstream iss(line);
		string server_ip, server_port;
		if (!(iss >> server_ip >> server_port)) { break; }
		if(number_of_servers==1){
			server_list.insert(make_pair(make_pair(atoi(server_port.c_str()),server_ip),
									make_pair(0+increment_range,256-1)));
		}else{
			server_list.insert(make_pair(make_pair(atoi(server_port.c_str()),server_ip),
									make_pair(0+increment_range,increment_range+range-1)));
		}
		increment_range += range;
		number_of_servers--;
	}
	infile.close();
	
	local_port=atoi(argv[1]);
	cout << local_port << "::" << "Total servers are: " << number_of_servers << " & Range for each is: " << range << endl;
	struct sockaddr_in socket_address;
    int server_ds, my_socket, read_count;
    socklen_t address_len = sizeof(socket_address);
    int option=1;
    char socket_buffer[1024] = {0};

    if ((server_ds = socket(AF_INET, SOCK_STREAM, 0)) <= 0){
        cerr << "ERROR: failed in creating socket" << endl;
        exit(EXIT_FAILURE);
    }
    if (setsockopt(server_ds, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &option, sizeof(option))) {
        cerr << "ERROR: failed setsockopt" << endl;
        exit(EXIT_FAILURE);
    }
    socket_address.sin_family = AF_INET;
    socket_address.sin_addr.s_addr = INADDR_ANY;
    socket_address.sin_port = htons(local_port); //htons(8080);

    if (bind(server_ds, (struct sockaddr *)&socket_address, sizeof(socket_address))<0) {
        cerr << "ERROR: binding failed" << endl;
        exit(EXIT_FAILURE);
    }
    if (listen(server_ds, 20) < 0) {
        cerr << "ERROR: failed at listen" << endl;
        exit(EXIT_FAILURE);
    }
	
	char hostname[1024];
	hostname[1023] = '\0';
	gethostname(hostname, 1023);
	struct hostent* host_n;
	host_n = gethostbyname(hostname);
	int socket_address_len = sizeof(struct sockaddr);
	
	struct ifaddrs *addrs, *tmp;
	if (getifaddrs(&addrs) == -1) {
		cerr << "ifaddrs is not set properly.. issue with environment setup!" << endl;
		exit(EXIT_FAILURE);
	}
	tmp = addrs;
	string eth0="eth0";
	while (tmp){
		if (tmp->ifa_addr && tmp->ifa_addr->sa_family == AF_INET){
			struct sockaddr_in *pAddr = (struct sockaddr_in *)tmp->ifa_addr;
			if(eth0.compare(string(tmp->ifa_name))==0){
				local_ip=inet_ntoa(pAddr->sin_addr);
			}
		}
		tmp = tmp->ifa_next;
	}
	freeifaddrs(addrs);

	cout << "Starting server.. " << endl;
	cout << "Host name: " << host_n->h_name << endl;
 	cout << "Server IP: " << local_ip << " & PORT: " << local_port << endl;
	
	// start a thread for read repair
	thread read_thread(read_repair_and_check);
	
	MapMessage local_map_msg;
	while(1)
	{
		if ((my_socket = accept(server_ds, (struct sockaddr *)&socket_address, (socklen_t*)&address_len))<0) {
			cerr << "ERROR: failed to accept" << endl;
			exit(EXIT_FAILURE);
		}

		read_count=read(my_socket, socket_buffer, 1024);
		if(read_count<0){
			cerr << "ERROR: In reading socket_buffer." << endl;
			return -1;
		}
		socket_buffer[read_count] = '\0';
		local_map_msg.ParseFromString(socket_buffer);
		
		if(local_map_msg.has_map_write()){
			cout << local_port << "::" << "Client sent write request as below" << endl;
			cout << local_port << "::" << "Key: " << local_map_msg.map_write().key();
			cout << " Value: " << local_map_msg.map_write().value();
			cout << " Write level: " << local_map_msg.map_write().write_level() << endl;
			cout << local_port << "::" << "Calling write function.." << endl;
			
			key_hintedHandoff++;
			writeToStore(local_map_msg.map_write(),key_hintedHandoff);
		}
		
		if(local_map_msg.has_map_replica_write()){
			
			if(switch_HintedHandoff){
				hinted_HandoffCheck(local_map_msg.map_replica_write().co_port(),local_map_msg.map_replica_write().co_ip());
			}

			replicaWrite(local_map_msg.map_replica_write());
			MapMessage return_message;
			WriteReplyFromReplica replica_return_write;
			replica_return_write.set_acknowledgment("ACK");
			replica_return_write.set_write_level(local_map_msg.map_replica_write().write_level());
			replica_return_write.set_client_ip(local_map_msg.map_replica_write().client_ip());
			replica_return_write.set_client_port(local_map_msg.map_replica_write().client_port());
			replica_return_write.set_server_ip(local_ip);
			replica_return_write.set_server_port(local_port);
			replica_return_write.set_key_hinted_handoff(local_map_msg.map_replica_write().key_hinted_handoff());
			return_message.set_allocated_map_replica_reply_write(&replica_return_write);
			string return_write_string;
			if (!return_message.SerializeToString(&return_write_string)) {
				cerr << "ERROR: Failed to return_message message." << endl;
				exit(EXIT_FAILURE);
			}
			{
				struct sockaddr_in address;
				int sock = 0, valread;
				struct sockaddr_in serv_addr;
				char buffer[1024] = {0};
				memset(&serv_addr, '0', sizeof(serv_addr));
				if ((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
					cerr << "ERROR: Socket creation error." << endl;
					exit(EXIT_FAILURE);
				}
				serv_addr.sin_family = AF_INET;
				serv_addr.sin_port = htons(local_map_msg.map_replica_write().co_port());
				if(inet_pton(AF_INET, local_map_msg.map_replica_write().co_ip().c_str(), &serv_addr.sin_addr)<=0) {
					cerr << "ERROR: Invalid address/ Address not supported." << endl;
					exit(EXIT_FAILURE);
				}
				if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
					cout << local_port << "::" << "Replica server not available for write on port:" << local_map_msg.map_replica_write().co_port() << endl;
				}else{
					if(send(sock, return_write_string.c_str(), return_write_string.size(), 0 ) != return_write_string.size()){ 
						cerr << "1.0 ERROR: In sending back socket_buffer." << endl;
						exit(EXIT_FAILURE);
					}
				}
				close(sock);
			}
			return_message.release_map_replica_reply_write();
		}

		if(local_map_msg.has_map_handoff_replica_write()){
			handoff_replicaWrite(local_map_msg.map_handoff_replica_write());
		}
		
		if(local_map_msg.has_map_read_repair_write()){
			repair_replicaWrite(local_map_msg.map_read_repair_write());
		}
		
		if(local_map_msg.has_map_read()){
			cout << local_port << "::" << "Client sent read request as below" << endl;
			cout << local_port << "::" << "Key:" << local_map_msg.map_read().key();
			cout << " Read level:" << local_map_msg.map_read().read_level() << endl;
			cout << local_port << "::" << "Calling read function.." << endl;
			
			key_readRepair++;
			readFromStore(local_map_msg.map_read(),key_readRepair);
		}
		
		if(local_map_msg.has_map_replica_read()){
			
			if(switch_HintedHandoff){
				hinted_HandoffCheck(local_map_msg.map_replica_read().co_port(),local_map_msg.map_replica_read().co_ip());
			}

			string return_string;
			MapMessage return_message;
			bool isExist=replicaRead(local_map_msg.map_replica_read());
			return_message.set_allocated_map_replica_reply_read(&replica_return_data);
			if (!return_message.SerializeToString(&return_string)) {
				cerr << "ERROR: Failed to return_message message." << endl;
				exit(EXIT_FAILURE);
			}
			{
				struct sockaddr_in address;
				int sock = 0, valread;
				struct sockaddr_in serv_addr;
				char buffer[1024] = {0};
				memset(&serv_addr, '0', sizeof(serv_addr));
				if ((sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0){
					cerr << "ERROR: Socket creation error." << endl;
					exit(EXIT_FAILURE);
				}
				serv_addr.sin_family = AF_INET;
				serv_addr.sin_port = htons(local_map_msg.map_replica_read().co_port());
				if(inet_pton(AF_INET, local_map_msg.map_replica_read().co_ip().c_str(), &serv_addr.sin_addr)<=0) {
					cerr << "ERROR: Invalid address/ Address not supported." << endl;
					exit(EXIT_FAILURE);
				}
				if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
					cout << local_port << "::" << "Replica server not available for read from port:" << local_map_msg.map_replica_read().co_port() << endl;
				}else{
					if(send(sock, return_string.c_str() , return_string.size() , 0 ) != return_string.size()){ 
						cerr << "1.0 ERROR: In sending back socket_buffer." << endl;
						exit(EXIT_FAILURE);
					}
				}
				close(sock);
			}
			return_message.release_map_replica_reply_read();
		}
		
		if(local_map_msg.has_map_replica_reply_read()){
			// make entry into 
			cout << local_port << "::" << "check point 1 READ reply from replica to co-ordinator." << endl;
			ReadReplyFromReplica t_RRFR=local_map_msg.map_replica_reply_read();
			serverValuesType temp_StoreValue;
			temp_StoreValue=make_pair(t_RRFR.key(),make_pair(t_RRFR.value(),t_RRFR.timestamp()));
			auto itr_R1 = readRepair.find(t_RRFR.key_read_repair());
			if(itr_R1 != readRepair.end()){
				//cout << local_port << "::" << "check point 4.5 Update for key_readRepair::::::" << t_RRFR.key_read_repair() << endl;
				itr_R1->second.first.second.second = itr_R1->second.first.second.second+1;
				replicaServerReply temp_map=itr_R1->second.second;
				temp_map.insert(make_pair(make_pair(t_RRFR.server_port(),t_RRFR.server_ip()),temp_StoreValue));
				itr_R1->second.second=temp_map;
			}else{
				//cout << local_port << "::" << "check point 4.6 Entry for key_readRepair::::::" << t_RRFR.key_read_repair() << endl;
				replicaServerReply temp_map;
				temp_map.insert(make_pair(make_pair(t_RRFR.server_port(),t_RRFR.server_ip()),temp_StoreValue));
				readRepair.insert(make_pair(t_RRFR.key_read_repair(),
						make_pair(make_pair(make_pair(t_RRFR.client_port(),t_RRFR.client_ip()),make_pair(t_RRFR.read_level(),1)),
						temp_map)));
			}
		}

		if(local_map_msg.has_map_replica_reply_write()){
			// make entry into 
			cout << local_port << "::" << "check point 1 WRITE reply from replica to co-ordinator." << endl;
			WriteReplyFromReplica t_WRFR=local_map_msg.map_replica_reply_write();
			auto itr_W1 = hintedHandoff.find(t_WRFR.key_hinted_handoff());
			if(itr_W1 != hintedHandoff.end()){
				//cout << local_port << "::" << "check point 4.5 Update for key_readRepair::::::" << t_WRFR.key_hinted_handoff() << endl;
				itr_W1->second.first.second.second = itr_W1->second.first.second.second+1;
				replicaServerWriteReply temp_map=itr_W1->second.second;
				temp_map.insert(make_pair(make_pair(t_WRFR.server_port(),t_WRFR.server_ip()),t_WRFR.acknowledgment()));
				itr_W1->second.second=temp_map;
			}else{
				//cout << local_port << "::" << "check point 4.6 Entry for key_readRepair::::::" << t_WRFR.key_hinted_handoff() << endl;
				replicaServerWriteReply temp_map;
				temp_map.insert(make_pair(make_pair(t_WRFR.server_port(),t_WRFR.server_ip()),t_WRFR.acknowledgment()));
				hintedHandoff.insert(make_pair(t_WRFR.key_hinted_handoff(),
						make_pair(make_pair(make_pair(t_WRFR.client_port(),t_WRFR.client_ip()),make_pair(t_WRFR.write_level(),1)),
						temp_map)));
			}
		}
		
		if(strcmp(socket_buffer,"NACK")==0){
			cout << local_port << "::" << "---------------------ERROR: check point 0.1 NACK received---------------------" << endl;
		}
		
		close(my_socket);
	} // while(1)

	// Optional:  Delete all global objects allocated by libprotobuf.
	google::protobuf::ShutdownProtobufLibrary();
	
	read_thread.join();

	return 0;
}

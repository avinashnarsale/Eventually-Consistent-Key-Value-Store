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
#include <list>
#include <thread>
#include <chrono> 
#include <random>
#include "map.pb.h"

using namespace std;

MapMessage map_message;
map<pair<string,int>,int>  server_list;

int coOrdinator_port = 0;
int receiving_port=0;
string receiving_ip;
string coOrdinator_ip;

void receiverFromServers(){
	struct sockaddr_in socket_address;
	memset(&socket_address, '0', sizeof(socket_address));
	int server_ds, my_socket, read_count, sock1;
	socklen_t address_len = sizeof(socket_address);
	int option=1;
	char socket_buffer[1024] = {0};

	//cout << "Check Point 1.1" << endl;
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
	socket_address.sin_port = htons(receiving_port); //htons(8080);

	//cout << "Check Point 1.2" << endl;
	if (bind(server_ds, (struct sockaddr *)&socket_address, sizeof(socket_address))<0) {
		cerr << "ERROR: binding failed" << endl;
		exit(EXIT_FAILURE);
	}
	if (listen(server_ds, 20) < 0) {
		cerr << "ERROR: failed at listen" << endl;
		exit(EXIT_FAILURE);
	}
		
	while(1)
	{
		
		if ((my_socket = accept(server_ds, (struct sockaddr *)&socket_address, (socklen_t*)&address_len))<0) {
			cerr << "ERROR: failed to accept" << endl;
			exit(EXIT_FAILURE);
		}

		//cout << "Client waiting for return data...." << endl;
		read_count=read(my_socket, socket_buffer, 1024);
		socket_buffer[read_count] = '\0';
		if(read_count<0){
			cerr << "ERROR: In reading buffer." << endl;
			exit(EXIT_FAILURE);
		}else{
			MapMessage return_message;
			return_message.ParseFromString(socket_buffer);
			if(return_message.has_map_metadata()){
				cout << "\nKey: " << return_message.map_metadata().key();
				cout << " Value: " << return_message.map_metadata().value();
				cout << " Timestamp: " << return_message.map_metadata().timestamp() << endl;
			}else{
				cout << "\nReply::" << socket_buffer << endl;
			}
		}
		close(my_socket);
	}
}


void writeRequest(){
	int input_key, input_wLevel;
	string input_value;
	cout << "Enter Key: ";
	cin >> input_key;
	if(input_key<0 || input_key > 255){
		cout << "Invalid key, enter within range - (Unsigned 0-255):";
		cin >> input_key;
	}
	if(input_key<0 || input_key > 255){
		cout << "Invalid again, exiting client.." << endl;
		exit(EXIT_FAILURE);
	}
	cin.ignore (std::numeric_limits<std::streamsize>::max(), '\n'); 
	cout << "Enter value: ";
	getline(cin, input_value);
	cout << "Enter consistency level: ";
	cin >> input_wLevel;
	if(input_wLevel<=0){
		cout << "Invalid consistency level, enter again:";
		cin >> input_wLevel;
	}
	if(input_wLevel<=0){
		cout << "Invalid again, exiting client.." << endl;
		exit(EXIT_FAILURE);
	}

	MapStoreWrite map_data;
	map_data.set_key(input_key);
	map_data.set_value(input_value);
	map_data.set_write_level(input_wLevel);
	map_data.set_client_port(receiving_port);
	map_data.set_client_ip(receiving_ip);
	
	string output;
	map_message.set_allocated_map_write(&map_data);
	if (!map_message.SerializeToString(&output)) {
		cerr << "ERROR: Failed to write map_data message." << endl;
		exit(EXIT_FAILURE);
	}
	map_message.release_map_write();

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
	serv_addr.sin_port = htons(coOrdinator_port);

	// Convert IPv4 and IPv6 addresses from text to binary form
	if(inet_pton(AF_INET, coOrdinator_ip.c_str(), &serv_addr.sin_addr)<=0) {
		cerr << "ERROR: Invalid address/ Address not supported." << endl;
		exit(EXIT_FAILURE);
	}
	if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
		cerr << "ERROR: Connection Failed." << endl;
		exit(EXIT_FAILURE);
	}
	send(sock, output.c_str() , output.size(), 0);
	cout << "Message sent to server: " << coOrdinator_port << " added successfully. " << endl;
	close(sock);
}

void readRequest(){
	int input_key, input_rLevel;
	string output_value,output_timestamp;
	cout << "Enter Key: ";
	cin >> input_key;
	if(input_key<0 || input_key > 255){
		cout << "Invalid key, enter within range - (Unsigned 0-255):";
		cin >> input_key;
	}
	if(input_key<0 || input_key > 255){
		cout << "Invalid again, exiting client.." << endl;
		exit(EXIT_FAILURE);
	}
	cout << "Enter consistency level: ";
	cin >> input_rLevel;
	if(input_rLevel<=0){
		cout << "Invalid consistency level, enter again:";
		cin >> input_rLevel;
	}
	if(input_rLevel<=0){
		cout << "Invalid again, exiting client.." << endl;
		exit(EXIT_FAILURE);
	}
	
	MapStoreRead map_data;
	map_data.set_key(input_key);
	map_data.set_read_level(input_rLevel);
	map_data.set_client_port(receiving_port);
	map_data.set_client_ip(receiving_ip);
	
	string output;
	map_message.set_allocated_map_read(&map_data);
	if (!map_message.SerializeToString(&output)) {
		cerr << "ERROR: Failed to read map_data message." << endl;
		exit(EXIT_FAILURE);
	}
	map_message.release_map_read();

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
	serv_addr.sin_port = htons(coOrdinator_port);

	// Convert IPv4 and IPv6 addresses from text to binary form
	if(inet_pton(AF_INET, coOrdinator_ip.c_str(), &serv_addr.sin_addr)<=0) {
		cerr << "ERROR: Invalid address/ Address not supported." << endl;
		exit(EXIT_FAILURE);
	}  
	if (connect(sock, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0){
		cerr << "ERROR: Connection Failed." << endl;
		exit(EXIT_FAILURE);
	}
	send(sock, output.c_str() , output.size(), 0);
	cout << "Request sent to server: " << coOrdinator_port << " added successfully. " << endl;
	close(sock);
}

int main(int argc, char* argv[]) {
	GOOGLE_PROTOBUF_VERIFY_VERSION;

	if (argc != 3) {
		cerr << "Usage: " << argv[0] << "./client <receive_port> <server_list_file>" << endl;
		return -1;
	}
	
	receiving_port=atoi(argv[1]);
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
				receiving_ip=inet_ntoa(pAddr->sin_addr);
			}
		}
		tmp = tmp->ifa_next;
	}
	freeifaddrs(addrs);
	ifstream infile(argv[2]);
	string line;
	int number_of_servers=0;
	while (getline(infile, line))
	{
		number_of_servers++;
		istringstream iss(line);
		string server_ip, server_port;
		if (!(iss >> server_ip >> server_port)) { break; }
		server_list.insert(make_pair(make_pair(server_ip,atoi(server_port.c_str())),number_of_servers));
	}
	infile.close();

	int temp_counter=0;
	for(auto itr=server_list.begin();itr!=server_list.end();++itr){
		temp_counter++;
		cout << temp_counter << ". => Port: "<< itr->first.second << " IP: " << itr->first.first << endl;
	}
	int select_server;
	cout << "Enter server number to select as CoOrdinator: ";
	cin >> select_server;
	temp_counter=0;
	for(auto itr=server_list.begin();itr!=server_list.end();++itr){
		temp_counter++;
		if(select_server==temp_counter){
			coOrdinator_ip=itr->first.first;
			coOrdinator_port=itr->first.second;
		}
	}
	
	// start thread for receiving replpies from Co-Ordinator/s
	thread receive_Thread(receiverFromServers);

	int select;
	while(1){
		cout << "---------->Menu<----------" << endl;
		cout << "1. Write \n2. Read \n3. Exit" << endl;
		cout << "Enter operation choice: ";
		cin >> select;
		
		if(select==3){
			cout << "Ending client..." << endl;
			exit(EXIT_SUCCESS);
			break;
		}
		
		switch(select){
			case 1:
				writeRequest();
				break;

			case 2:
				readRequest();
				break;

			default:
				cout << "Enter valid choice. " << endl;
				break;
		}
	}
	// Optional:  Delete all global objects allocated by libprotobuf.
	google::protobuf::ShutdownProtobufLibrary();
	receive_Thread.join();
	return 0;
}

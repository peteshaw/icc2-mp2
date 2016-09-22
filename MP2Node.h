/**********************************
 * FILE NAME: MP2Node.h
 * @author harish2
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:        string value2 = "";
        MessageType type;

 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:

	// Ring
	vector<Node> ring;
	// Hash Table
    HashTable * kvsHashTable;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;

	int initialized;

	//Temporary Vectors which hold the values when the ring is reconstructed
	//and when the stabalization protocol is called

    struct Transaction {
        int time = 0;
        string key = "";
        string value = "";
        string value2 = "";
        MessageType type;
        int  count = 0;
        int failures = 0;
        bool complete= false;
    };



    //Maps the transaction id to values associated with it
    map<long, Transaction> transactions;




public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
    size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDeleteKey(string key);

	// receive messages from Emulnet 
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

	~MP2Node();

	//A function to increment the transactionID counter after each unique CRUD operation
	void clientCreateMap(string key, string value);
	void clientDeleteMap(string key);

	void createPrimaryKeyValHelper(string key);
	void createKeyValuePrimary(string key);
    void createKeyValueSecondary(string key);
    void createKeyValueTertiary(string key);

	//Message Handler Functions
    void createMessageHandler(Message incomingMessage);
    void readMessagehandler(Message incomingMessage);
    void updateMessagehandler(Message incomingMessage);
    void deleteMessagehandler(Message incomingMessage);

	//Message Reply Handler functions

    void readReplyMessageHandler(Message incomingMessage);
    void createReplyMessageHandler(Message incomingMessage);
    void updateReplyMessageHandler(Message incomingMessage);
    void deleteReplyMessageHandler(Message incomingMessage);

	//Transaction TImeout handler function
    void cleanUpTransactions();


    //utility functions
    bool isAddressInMembership(Address node);
    Address makeAddress(int id, short port);
    int getMyId();
    short getMyPort();
    int getAddressId(Address addr);
    short getAddressPort(Address addr);
    long addTransaction(string key, string value, MessageType type );
    void trace(const string &function, long transaction, Address myAddress, const string &key, const string &value, const string &description);
};

#endif /* MP2NODE_H_ */

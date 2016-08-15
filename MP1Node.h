/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Header file of MP1Node class.
 **********************************/

#ifndef _MP1NODE_H_
#define _MP1NODE_H_

#include "stdincludes.h"
#include "Log.h"
#include "Params.h"
#include "Member.h"
#include "EmulNet.h"
#include "Queue.h"

/**
 * Macros
 */
#define TREMOVE 20
#define TFAIL 5
#define SPREAD_RATE 3

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Message Types
 */
enum MessageTypes{
    JOINREQ,
    JOINREP,
    MEMBER_TABLE,
};

/**
 * STRUCT NAME: MessageHeader
 *
 * DESCRIPTION: Header and content of a message
 */
typedef struct Message {
    enum MessageTypes messageType;
    Address address;
    long heartbeat;
    vector<MemberListEntry> memberList;
} Message;

/**
 * CLASS NAME: MP1Node
 *
 * DESCRIPTION: Class implementing Membership protocol functionalities for failure detection
 */
class MP1Node {
private:
	EmulNet *emulNet;
	Log *log;
	Params *par;
	Member *memberNode;
	char NULLADDR[6];

public:
	MP1Node(Member *, Params *, EmulNet *, Log *, Address *);
	Member * getMemberNode() {
		return memberNode;
	}
    int getAddressId(Address addr);
    short getAddressPort(Address addr);
    int getMyId();
    short getMyPort();
    int recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);
	void nodeStart(char *servaddrstr, short serverport);
	int initThisNode(Address *joinaddr);
	int introduceSelfToGroup(Address *joinAddress);
	int finishUpThisNode();
	void nodeLoop();
	void checkMessages();
	bool recvCallBack(void *env, char *data, int size);
	void nodeLoopOps();
	int isNullAddress(Address *addr);
	Address getJoinAddress();
    void initMemberListTable(Member *memberNode, int id, short port);
	void printAddress(Address *addr);
	virtual ~MP1Node();
    void handleJoinRequest(Message *message);
    void handleJoinReply(Message *message);
    void handleMemberTable(Message *message);
    void logMemberStatus();
    void updateMemberList(int id, short port, long heartbeat);
    Address makeAddress(int id, short port);
    void sendMemberTables();
};

#endif /* _MP1NODE_H_ */

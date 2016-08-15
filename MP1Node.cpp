/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <sstream>
#include <time.h>

using namespace std;

/**********************************
 *
 * My Modified Code
 *
 **********************************/

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */



int MP1Node::initThisNode(Address *joinAddress) {
    /*
     * This function is partially implemented and may require changes
     */


    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode,getAddressId(*joinAddress),getAddressPort(*joinAddress));

    stringstream ss;
    cout << "initThisNode: id=" << getMyId() << "| port=" << getMyPort() << endl;

    return 0;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *envelope, char *data, int size ) {

    /*
     * the envelope is a pointer to a Message struct, containing:
            Message Header
            An Address
            An Optional Member Table
    */

    Message* message = (Message*) data;

    switch (message->messageType) {
    case JOINREQ:
        handleJoinRequest(message);
        break;
    case JOINREP:
        handleJoinReply(message);
        break;
    case MEMBER_TABLE:
        handleMemberTable(message);
        break;
    default:
        log->LOG(&memberNode->addr, "Received other msg");
        break;
    }
    return 0;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

    // set timeout value
    int timeout = TFAIL;

    //increment heartbeat
    memberNode->heartbeat += 1;

    //check for failed nodes and delete them
    for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
         it != memberNode->memberList.end();) {
        // compare duration since last timestamp and timeout value
        if (par->getcurrtime() - it->timestamp > timeout) {
            Address addr = makeAddress(it->id, it->port);
            cout  << "Timing out " << addr.getAddress() << endl;
            log->logNodeRemove(&memberNode->addr, &addr);
            memberNode->memberList.erase(it);
        } else {
            it++;
        }
    }

    //increment the heartbeat
    memberNode->heartbeat += 1;

    //now update informatino for this node
    updateMemberList(getMyId(), getMyPort(), memberNode->heartbeat);

    //send some member tables out to peers
    sendMemberTables();

    //send out member table messages to a few folks
    return;
}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
    /*
    * Your code goes here
    */
    return 0;
}

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**********************************
 *
 * Lib calls and functions
 *
 **********************************/

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for( int i = 0; i < 6; i++ ) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    cout << "nodeStart: " << this->memberNode->addr.getAddress() << " joinaddr=" << joinaddr.getAddress() << endl;

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}



/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *newNodeAddress) {
    Message *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif
    int joinId = getAddressId(*newNodeAddress);
    short joinPort = getAddressPort(*newNodeAddress);

    //message always has the address of the sender

    if  (   (joinId == getMyId())
            && (joinPort == getMyPort()))
    {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif

        memberNode->inGroup = true;
        //add self to group
    }
    else {
        //send a message to the coordinator

        msg = new Message;
        size_t msgsize = sizeof(Message);

        // create JOINREQ message: format of data is {struct Address myaddr}
        // sending the join request FROM ME  (originator) to
        msg->messageType = JOINREQ;
        msg->address = memberNode->addr;
        //cout << "introduceSelfToGroup:member address is " << memberNode->addr.getAddress() << endl;
        msg->heartbeat = memberNode->heartbeat;

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        cout << "introduceSelfToGroup:sending Join Mesage from " << memberNode->addr.getAddress() << endl;
        emulNet->ENsend(&memberNode->addr, newNodeAddress, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}


/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }
    //int id = *(int*)(&memberNode->addr.addr);
    //cout << "nodeLoop for id" << id << ": inGroup=" << memberNode->inGroup << endl;

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
        return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}


/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 * also adds an entry for the join address, and a second entry for the node itself
 */
void MP1Node::initMemberListTable(Member *memberNode, int id, short port) {
    memberNode->memberList.clear();

    //add join address
    MemberListEntry joinNode = MemberListEntry(id, port);
    joinNode.settimestamp(par->getcurrtime());
    joinNode.setheartbeat(memberNode->heartbeat);
    memberNode->memberList.push_back(joinNode);

    //add yourself to the list
    MemberListEntry myNode = MemberListEntry(getMyId(), getMyPort());
    myNode.settimestamp(par->getcurrtime());
    myNode.setheartbeat(memberNode->heartbeat);
    memberNode->memberList.push_back(myNode);

}


/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",
           addr->addr[0],
            addr->addr[1],
            addr->addr[2],
            addr->addr[3],
            *(short*)&addr->addr[4]) ;
}

void MP1Node::handleJoinRequest(Message *mRequest) {

    //create a join reply and send it
    Message *mReply;
    mReply = new Message;
    size_t msgsize = sizeof(Message);


    mReply->messageType = JOINREP;
    mReply->heartbeat = memberNode->heartbeat;
    mReply->address=mRequest->address;

    updateMemberList(getAddressId(mReply->address), getAddressPort(mReply->address), mRequest->heartbeat);

    logMemberStatus();

    cout << "handleJoinrequest:Sending JOINREP from ";
    cout << mRequest->address.getAddress();
    cout << "to " << memberNode->addr.getAddress();
    cout << ":heartbeat=" << memberNode->heartbeat << endl;

    emulNet->ENsend(&memberNode->addr, &(mReply->address), (char *)mReply, msgsize);
    free(mReply);
    free(mRequest);
}

void MP1Node::handleJoinReply(Message *message) {

    //got a reply so we are in the group now
    memberNode->inGroup = true;

    // update the heartbeat and the member table
    Address sender = message->address;
    int id = getAddressId(sender);
    short port = getAddressPort(sender);
    updateMemberList(id, port, message->heartbeat);
    cout << "handleJoinReply: reply from " << sender.getAddress() << endl;
}



void MP1Node::handleMemberTable(Message *message) {

    vector<MemberListEntry>::iterator mlItem = message->memberList.begin();
    for (; mlItem != message->memberList.end(); ++mlItem) {
        int id = mlItem->getid();
        short port = mlItem->getport();
        long heartbeat = mlItem->getheartbeat();
        updateMemberList(id, port, heartbeat);
    }

    cout << "handleMemberTable from " << message->address.getAddress();
}

void MP1Node::updateMemberList(int id, short port, long heartbeat)  {

    //given a new node with id, port, and heartbeat, add it to the member list

    //if the node exists then update it
    //loop through the list and find
    vector<MemberListEntry>::iterator it = memberNode->memberList.begin();

    for (; it != memberNode->memberList.end(); ++it) {
        if (it->id == id && it->port ==port) {
            if (heartbeat > it->heartbeat) {
                it->setheartbeat(heartbeat);
                it->settimestamp(par->getcurrtime());
            }
            //cout << "updatememberlist: updating " << id << ":" << port << ":" << heartbeat << endl;
            return;
        }
    }

    //item not found in list, so add it.
    MemberListEntry memberEntry(id, port, heartbeat, par->getcurrtime());
    memberNode->memberList.push_back(memberEntry);

    Address newMember = makeAddress(id, port);
    log->logNodeAdd(&memberNode->addr, &newMember);

    //cout << "updatememberlist: adding " << id << ":" << port << ":" << heartbeat << endl;
    logMemberStatus();
}



void MP1Node::sendMemberTables()
{
    int sendCount = SPREAD_RATE;

    Message *message;
    message = new Message;

    message->messageType = MEMBER_TABLE;

    //copy the address - the sent message gets assigned the FROM address
    message->address = memberNode->addr;

    //copy the menber list table
    message->memberList = memberNode->memberList;

    //copy the heartbeat
    message->heartbeat = memberNode->heartbeat;

    size_t messageSize = sizeof(Message);

    srand(time(0)); // use current time as seed for random generator

    int listSize = memberNode->memberList.size();

    //as long as i is less than sendCount AND the length of the array, loop through and send random tables.
    for (int i = 1; ((i < sendCount) && (i < listSize)); i++) {
            int rnd = rand();
            int item = rnd % listSize;
            MemberListEntry mleItem = memberNode->memberList[item];
            if((mleItem.id != getMyId())) {
                Address destination = makeAddress(mleItem.id, mleItem.port);
                emulNet->ENsend(&memberNode->addr, &destination, (char *)message, messageSize);
                cout << "sending member table from " << getMyId() << ":to:" << mleItem.id << endl;
        }
    }
}

/******************************************************************************
 *
 * Utility Functions
 *
 *
 *****************************************************************************/

Address MP1Node::makeAddress(int id, short port) {
    Address addr;
    memcpy(&addr.addr[0], &id, sizeof(int));
    memcpy(&addr.addr[4], &port, sizeof(short));
    return addr;
}

int MP1Node::getAddressId(Address myAddress) {

    int id;
    memcpy(&id, &myAddress.addr, sizeof(int));
    return id;
}

short MP1Node::getAddressPort(Address myAddress) {
    short port;
    memcpy(&port, &myAddress.addr[4], sizeof(short));
    return port;
}

int MP1Node::getMyId() {

    return this->getAddressId(memberNode->addr);

}

short MP1Node::getMyPort() {

    return this->getAddressPort(memberNode->addr);
}

void MP1Node::logMemberStatus() {
    cout << "for node " << memberNode->addr.getAddress() << "==>" << endl;
    cout << "heartbeat = " << memberNode->heartbeat << endl;
    cout << "[";
    for (vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); it != memberNode->memberList.end(); it++) {
        cout << it->getid() << ":" << it->getheartbeat() << ":" << it->gettimestamp() << ",";
    }
    cout << "]" << endl;
}


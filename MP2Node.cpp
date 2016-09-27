/**********************************
 * FILE NAME: MP2Node.cpp
 * author petes shaw
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"
#include <sstream>

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
    this->memberNode = memberNode;
    this->par = par;
    this->emulNet = emulNet;
    this->log = log;
    kvsHashTable = new HashTable();
    this->memberNode->addr = *address;
    initialized = 0;
}

/**

 * Destructor
 */
MP2Node::~MP2Node() {
    delete kvsHashTable;
    delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Checks for changes in the ring, and if there are, calls the stabilization protocol
 */
void MP2Node::updateRing() {

    vector<Node> currentMembersList;
    currentMembersList = getMembershipList();
    // Sort the list based on the hashCode
    sort(currentMembersList.begin(), currentMembersList.end());

    bool bChange = false;

    if ( ring.size() == 0 ){
        ring=currentMembersList;
        return;
    }

    if (currentMembersList.size() != ring.size()){
        bChange = true;
    } else {
        for(int i = 0; i < currentMembersList.size(); i++) {
              if (!( ring[i].nodeAddress == currentMembersList[i].nodeAddress)) {
                  bChange = true;
                  break;
              }
          }
    }

    ring=currentMembersList;

    if (bChange == true) {
        stabilizationProtocol();
    }
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
    vector<Node> curMemList;
    for ( auto i = 0 ; i < (int)(this->memberNode->memberList.size()); i++ ) {
        Address addressOfThisMember;
        int id = this->memberNode->memberList.at(i).getid();
        short port = this->memberNode->memberList.at(i).getport();
        addressOfThisMember = makeAddress(id, port);
        curMemList.emplace_back(Node(addressOfThisMember));
    }
    return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
    std::hash<string> hashFunc;
    size_t ret = hashFunc(key);
    return ret % RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 *
 * create a transaction to track responses
 * send out create message to the primary, secondary and tertiary nodes.
 * if I am one of the three then create myself.
 *
 */
void MP2Node::clientCreate(string key, string value) {

    //Get the vector which contains the three nodes where the key will map to
    //Create a message and send it to the respective node

    vector<Node> replicaNodes = findNodes(key);


    long tID = addTransaction(key, value, CREATE);
    Transaction t = transactions[tID];
    string fName = "clientCreate";
    //New Primary Node
        Message msg(tID, memberNode->addr, CREATE, key, value, PRIMARY);
        emulNet->ENsend(&memberNode->addr, replicaNodes[0].getAddress(), msg.toString());
        string s = "PRIMARY create to " + replicaNodes[0].getAddress()->getAddress();
        trace(fName,tID, memberNode->addr,key, value, s);

    //new secondary node
        Message msg2(tID, memberNode->addr, CREATE, key, value, SECONDARY);
        emulNet->ENsend(&memberNode->addr, replicaNodes[1].getAddress(), msg2.toString());
        s = "SECONDARY create to " + replicaNodes[0].getAddress()->getAddress();
        trace(fName,tID, memberNode->addr,key, value, s);


    //new tertiary node
        Message msg3(tID, memberNode->addr, CREATE, key, value, TERTIARY);
        emulNet->ENsend(&memberNode->addr, replicaNodes[2].getAddress(), msg3.toString());
        s = "TERTIARY create to " + replicaNodes[0].getAddress()->getAddress();
        trace(fName,tID, memberNode->addr,key, value, s);


    //update the transaction list
    transactions[tID] = t;

}



/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 *
 */
void MP2Node::clientRead(string key){


    string fName = "clientRead";
    vector<Node> replicas = findNodes(key);
    long tID = addTransaction(key, "", READ);
    Transaction t = transactions[tID];
    string value = "";

    for(auto i = 0; i<(int)replicas.size(); i++){
        Message msg(g_transID, memberNode->addr, READ, key);
        emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
        Node thisNode = replicas[i];
        Address fromAddr  = *(thisNode.getAddress());
        string sFromAddr = fromAddr.getAddress();
        string s = "sending read message to " + sFromAddr;
        trace(fName,tID, memberNode->addr,key,value, s);
    }
    transactions[tID]= t;
}


/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){

    long tID = addTransaction(key, value, UPDATE);
    string fName = "client update";

    vector<Node> replicas = findNodes(key);
    if(replicas.size() == 0){
        log->logUpdateFail(&memberNode->addr, true, g_transID, key, value);
        trace(fName,tID, memberNode->addr,key, value, "update fail");
        return;
    }

    for(auto i = 0; i<(int)replicas.size(); i++){
            Message msg(tID, memberNode->addr, UPDATE, key, value);
            emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
            Node n = replicas[i];
            Address a = *(n.getAddress());
            string s = "sending update to " + a.getAddress();
            trace("clientUpdate",tID, memberNode->addr,key, value, s);

        }
    }

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDeleteKey(string key){
    /*
     * Implement this
     */

    vector<Node>  replicas = findNodes(key);

    long tID = addTransaction(key, "", DELETE);
    Transaction t = transactions[tID];

    for(auto i = 0; i<(int)replicas.size(); i++){

        Message msg(g_transID, memberNode->addr, DELETE, key);
        emulNet->ENsend(&memberNode->addr, replicas[i].getAddress(), msg.toString());
    }
    transactions[tID] = t;
}


/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {

    // Update key in local hash table and return true or false

    string read = kvsHashTable->read(key);

    if( read == ""){
        return false;
    }
    else{

        return kvsHashTable->update(key, value);

    }

}


/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {

    char * data;
    int size;

    // dequeue all messages and handle them
    while ( !memberNode->mp2q.empty() ) {
        /*
         * Pop a message from the queue
         */
        data = (char *)memberNode->mp2q.front().elt;
        size = memberNode->mp2q.front().size;
        memberNode->mp2q.pop();

        string message(data, data + size);


        /*
         * Handle the message types here
         */

        Message incomingMessage(message);  
        long tID = incomingMessage.transID;
        string fName = "checkMessages";
        string fromAddress = incomingMessage.fromAddr.getAddress();
        Address node = memberNode->addr;
        string s = "";
        string blank = "";


        bool bTransactionComplete = false;
        //skip any transaction that is marked complete
        if (transactions.find(tID) != transactions.end()) {
            if (transactions[tID].complete == true) {
                bTransactionComplete = true;
            }
        }

        //Handle the CREATE message
        if(incomingMessage.type == CREATE) {
            createMessageHandler(incomingMessage);
            s = "incoming CREATE message from " + fromAddress;
                trace(fName,tID,node,blank ,blank,s);
        }

        //Handle the READ message
        else if(incomingMessage.type == READ){
            readMessagehandler(incomingMessage);
            s = "incoming READ message from " + fromAddress;
                trace(fName,tID,node,blank ,blank,s);
        }

        //Handle the UPDATE message
        else if(incomingMessage.type == UPDATE) {
            updateMessagehandler(incomingMessage);
            s = "incoming UPDATE message from " + fromAddress;
            trace(fName,tID,node,blank ,blank,s);
        }

        //Handle DELETE message
        else if(incomingMessage.type == DELETE){
            deleteMessagehandler(incomingMessage);
            s = "incoming DELETE message from " + fromAddress;
            trace(fName,tID,node,blank ,blank,s);
        }

        //Handle a reply message
        else if(incomingMessage.type == REPLY){

            if (bTransactionComplete == false) {

                MessageType type = transactions[tID].type;

                //REPLY of a CREATE
                if(type == CREATE){
                    createReplyMessageHandler(incomingMessage);
                    s = "incoming CREATE REPLY message from " + fromAddress;
                    trace(fName,tID,node,blank ,blank,s);
                }

                //REPLY of a UPDATEis
                else if(type == UPDATE){
                    updateReplyMessageHandler(incomingMessage);
                    s = "incoming UPDATE REPLY message from " + fromAddress;
                    trace(fName,tID,node,blank ,blank,s);
                }

                //REPLY of a DELETE
                else if(type == DELETE){
                    s = "incoming DELETE REPLY message from " + fromAddress;
                    deleteReplyMessageHandler(incomingMessage);
                    trace(fName,tID,node,blank ,blank,s);
                }
            }
        }

        //Handle READREPLY
        else if(incomingMessage.type == READREPLY){
            if (bTransactionComplete == false) {
                readReplyMessageHandler(incomingMessage);
                s = "incoming READ REPLY message from " + fromAddress;
                trace(fName,tID,node,blank ,blank,s);
            }
        }
    }
    //Timeout on requests that you have been waiting too long on
    cleanUpTransactions();
}


/*
 * FUNCTION: cleanupTransactions
 * Helper function to reject transactions which have times out
 * also delete completed transactions
*/
void MP2Node::cleanUpTransactions() {

    map<long, Transaction>::iterator it = transactions.begin();



    //delete and log timed out transactions
    while(it != transactions.end()){
        long transactionID = it->first;
        Transaction t = it->second;

        if((par->getcurrtime() - t.time) > 10){


            //erase completed messages
            //log ones that time out as incomplete

            if (t.complete != true) {

                //The original message was a CREATE message
                if(t.type == CREATE) {
                    log->logCreateFail(&memberNode->addr, true, transactionID, t.key, t.value);
                }

                //The original message was a READ message
                else if(t.type == READ) {
                    log->logReadFail(&memberNode->addr, true, transactionID, t.key);
                }

                //The original message was a DELETE message

                else if(t.type == DELETE){
                    log->logDeleteFail(&memberNode->addr, true, transactionID, t.key);
                }

                //The original message was a UPDATE message
                else if(t.type == UPDATE){
                    log->logUpdateFail(&memberNode->addr, true, transactionID, t.key, t.value);
                }
            }

            //We will clear the associated elements from the map
            //note - ned to increment the iterator otherwise it won't work
            it = transactions.erase(it);
        }
        //just iterate over simply as nothing was delete
        else{
            ++it;
        }

    }

}

/*
*   FUNCTION: readReplyMessageHandler
*
*   DESCRIPTION: as the responses come in...
*   1 - if we have two matching entries, the read is a success
*   2 - else the read fails
*
*/
void MP2Node::readReplyMessageHandler(Message incomingMessage){

    long tID = incomingMessage.transID;
    Transaction t = transactions[tID];
    string key = incomingMessage.key;
    string value = incomingMessage.value;

    //THis is the first message so far
    // put the received value in a map and update the count
    t.count++;

    //fail if we get 2 failed reads
    if (value != "_") {
        t.failures++;
        if (t.failures > 1) {
            log->logReadFail(&memberNode->addr, true, incomingMessage.transID, t.key);
            //cout << "read fail" << tID << ", " << memberNode->addr.getAddress() << ", " << t.key << ", " << endl;
        }
    }

   //try and find a match or quorum    
    if (t.count == 1) {
        t.value = value;
    }

    if (t.count == 2) {
        if (t.value == value) {
            t.complete = true;
            log->logReadSuccess(&memberNode->addr, true, tID, t.key, incomingMessage.value);
            //cout << "read success " << tID << ", " << memberNode->addr.getAddress() << ", " << t.key << ", " << incomingMessage.value << endl;
        } else {
            t.value2 = value;
        }
    }

    if (t.count == 3) {
        t.complete = true;
        if ((t.value == value) || (t.value2 == value)) {
            log->logReadSuccess(&memberNode->addr, true, tID, t.key, incomingMessage.value);
            //cout << "read success " << tID << ", " << memberNode->addr.getAddress() << ", " << t.key << ", " << incomingMessage.value << endl;
        } else {
            log->logReadFail(&memberNode->addr, true, incomingMessage.transID, t.key);
            //cout << "read fail" << tID << ", " << memberNode->addr.getAddress() << ", " << t.key << ", " << endl;
        }
    }

    transactions[incomingMessage.transID] = t;
}



/*
* Helper Function for handling DELETE REPLY messages
*/
void MP2Node::deleteReplyMessageHandler(Message incomingMessage){


    Transaction t = transactions.find(incomingMessage.transID)->second;

    //The reply was a success reply
    if(incomingMessage.success){
        //We have received the second success message, quorum has been reached
        //Mark this transaction as done, and log success
        if(t.count>=1){
            log->logDeleteSuccess(&memberNode->addr, true, incomingMessage.transID, t.key);
            t.complete = true;
        }
        //The quorum has not been reached yet
        else {
            t.count ++;
        }
    }
    //The reply was a filure
    else {
        //We have received the second failure message, quorum has been reached
        //Mark this transaction as done, and log failure
        if(t.failures>=1){
            log->logDeleteFail(&memberNode->addr, true, incomingMessage.transID, t.key);
            t.complete = true;
        }
        //The quorum has not been reached yet
        else{
            t.count++;
            t.failures++;
        }
    }
    transactions[incomingMessage.transID] = t;
}

/*
* Helper Function for handling UPDATE REPLY messages
*/
void MP2Node::updateReplyMessageHandler(Message incomingMessage){

    Transaction t = transactions.find(incomingMessage.transID)->second;

    //The reply was a success reply
    if(incomingMessage.success){
        //We have received the second success message, quorum has been reached
        //Mark this transaction as done, and log success
        if(t.count >= 1){
            log->logUpdateSuccess(&memberNode->addr, true, incomingMessage.transID, t.key, t.value);
            t.complete = true;
        }
        //The quorum has not been reached yet
        else{
            t.count++;
        }
    }
    //The reply was a filure
    else{
        //We have received the second failure message, quorum has been reached
        //Mark this transaction as done, and log failure
        if(t.failures >= 1){
            log->logUpdateFail(&memberNode->addr, true, incomingMessage.transID, t.key, t.value);
            t.complete = true;
        }
        //The quorum has not been reached yet
        else{
            t.failures++;
            t.count++;
        }
    }
    transactions[incomingMessage.transID] = t;
}


/*
* FUNCTION: createReplyMessageHandler
*
* DESCRIPTION: when a clientCreate request is recieved, we become the coordinator of this transaction
* 3 creates are sent out.
* so we count the replies, and if we recieve at least two, log a successful create
*/
void MP2Node::createReplyMessageHandler(Message incomingMessage){

    Transaction t = transactions.find(incomingMessage.transID)->second;

    //cout << "got createReply -from:" << incomingMessage.fromAddr.getAddress() << " to:"  << memberNode->addr.getAddress() << endl;

    //The reply was a success
    if(incomingMessage.success){
        //We have received the second success message, quorum has been reached
        //Mark this transaction as done, and log success
        if(t.count >= 1){
            log->logCreateSuccess(&memberNode->addr, true, incomingMessage.transID, t.key, t.value);
            //cout << "create success," << incomingMessage.transID << "," << t.key << "," << t.value << endl;
            t.complete = true;
        }
        //The quorum has not been reached yet
        else{
            t.count++;
            //cout << "quorum not reached, count = " << t.count << "trans:" << incomingMessage.transID << "," << t.key << "," << t.value << endl;
        }
    }
    //The reply was a failure
    else{
        //We have received the second failure message, quorum has been reached
        //Mark this transaction as done, and log failure
        if(t.failures >= 1){
            log->logCreateFail(&memberNode->addr, true, incomingMessage.transID, t.key, t.value);
            //cout << "create fail," << incomingMessage.transID << "," << t.key << "," << t.value << endl;
            t.complete=true;
        }
        //The quorum has not been reached yet
        else{
            t.failures++;
            t.count++;
        }
    }

    transactions[incomingMessage.transID] = t;
}


/*
* Helper Function for handling UPDATE messages
*/
void MP2Node::updateMessagehandler(Message incomingMessage){

    //Get the message variables
    Address fromAddr =  incomingMessage.fromAddr;
    string key = incomingMessage.key;
    string value = incomingMessage.value;
    ReplicaType replica = incomingMessage.replica;

    //update the key
    bool retVal = updateKeyValue(key, value, replica);

    //send the message reply
    Message msg(incomingMessage.transID, memberNode->addr, REPLY, retVal);
    emulNet->ENsend(&memberNode->addr, &fromAddr, msg.toString());
}




/*
* Helper Function for handling DELETE messages
*/

void MP2Node::deleteMessagehandler(Message incomingMessage){

    //Get the message variables
    Address fromAddr =  incomingMessage.fromAddr;
    string key = incomingMessage.key;

    bool retVal = kvsHashTable->deleteKey(key);

    Message msg(incomingMessage.transID, memberNode->addr, REPLY, retVal);
    emulNet->ENsend(&memberNode->addr, &fromAddr, msg.toString());

}


/*
* FUNCTION: readMessageHandler
*
* DESCRIPTION: the coordinator has sent me a message, so I either reply with the value, or "_" if I fail.
*
*
*/
void MP2Node::readMessagehandler(Message incomingMessage){


    //Get the message variables
    Address fromAddr =  incomingMessage.fromAddr;
    string key = incomingMessage.key;
    string value = kvsHashTable->read(key);
    long tID = incomingMessage.transID;


    //The key was found, Log success and send the message
    if(value != ""){
        Message msg(incomingMessage.transID, memberNode->addr, value);
        emulNet->ENsend(&memberNode->addr, &fromAddr, msg.toString());
        log->logReadSuccess(&memberNode->addr, false, incomingMessage.transID, key, value);
        cout << "read msg-found" << tID << ", " << memberNode->addr.getAddress() << ", " << key << ", " << value << endl;
    }
    //A key was not found, Log failure and send the message
    else{
        Message msg(incomingMessage.transID, memberNode->addr, "_");
        emulNet->ENsend(&memberNode->addr, &fromAddr, msg.toString());
        log->logReadFail(&memberNode->addr, false, incomingMessage.transID, key);
        cout << "read msg-fail" << tID << ", " << memberNode->addr.getAddress() << ", " << key << endl;
    }
}

/*
* Helper Function for handling CREATE messages
*/
void MP2Node::createMessageHandler(Message incomingMessage){

    //Get the message variables
    Address fromAddr =  incomingMessage.fromAddr;
    string key = incomingMessage.key;
    string value = incomingMessage.value;

    //Create a KeyValue
    bool retVal = kvsHashTable->create(key, value);


    //Create and send the message
    Message msg(incomingMessage.transID, memberNode->addr, REPLY, retVal);
    emulNet->ENsend(&memberNode->addr, &fromAddr, msg.toString());

    //log yet another successful operation
    log->logCreateSuccess(&memberNode->addr, true, incomingMessage.transID, key, value);

}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 * returns a vector with PRIMARY, SECONDARY, AND TERTIARY
 */
vector<Node> MP2Node::findNodes(string key) {
    size_t pos = hashFunction(key);
    vector<Node> addr_vec;
    if (ring.size() >= 3) {
        // if pos <= min || pos > max, the leader is the min
        if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
            addr_vec.emplace_back(ring.at(0));
            addr_vec.emplace_back(ring.at(1));
            addr_vec.emplace_back(ring.at(2));
        }
        else {
            // go through the ring until pos <= node
            for (auto i=1; i<(int)ring.size(); i++){
                Node addr = ring.at(i);
                if (pos <= addr.getHashCode()) {
                    addr_vec.emplace_back(addr);
                    addr_vec.emplace_back(ring.at((i+1)%ring.size()));
                    addr_vec.emplace_back(ring.at((i+2)%ring.size()));
                    break;
                }
            }
        }
    }
    return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
        return false;
    }
    else {
        return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}


/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 *              Compare the Old and New replicas vectors, and see if there were any changes.
 */
void MP2Node::stabilizationProtocol() {

    map<string, string>::iterator it;
    vector<Node> failedNodes;
    string fName = "stabilizationProtocoal";

    for(it = kvsHashTable->hashTable.begin(); it != kvsHashTable->hashTable.end(); it++){

        string key = it->first;
        string value = it->second;
        long tID = addTransaction(key, value, CREATE);
        string blank = "";

        //get replicas for the current key
        vector<Node> replicaNodes = findNodes(it->first);

        Message msg1 (tID, memberNode->addr, CREATE, key, value, PRIMARY);
        emulNet->ENsend(&memberNode->addr, replicaNodes[0].getAddress(), msg1.toString());
        Node n = replicaNodes[0];
        Address a = *n.getAddress();
        string s = "sending create message to" + a.getAddress();
        trace(fName,tID,memberNode->addr,key,blank, s);


        Message msg2 (tID, memberNode->addr, CREATE, key, value, SECONDARY);
        emulNet->ENsend(&memberNode->addr, replicaNodes[1].getAddress(), msg2.toString());
        n = replicaNodes[1];
        a = *n.getAddress();
        s = "sending create message to" + a.getAddress();
      trace(fName,tID,memberNode->addr,key,blank, s);


        Message msg3 (tID, memberNode->addr, CREATE, key, value, TERTIARY);
        emulNet->ENsend(&memberNode->addr, replicaNodes[2].getAddress(), msg3.toString());
        n = replicaNodes[2];
        a = *n.getAddress();
        s = "sending create message to" + a.getAddress();
        trace(fName,tID,memberNode->addr,key,blank, s);

       }
}


/*    //loop through the local store
    for(it = kvsHashTable->hashTable.begin(); it != kvsHashTable->hashTable.end(); it++){

        string key = it->first;
        string value = it->second;

        //get replicas for the current key
        vector<Node> replicaNodes = findNodes(it->first);

        //case: 1 - I AM THE PRIMARY
        if (replicaNodes[0].getAddress() == &(memberNode->addr)) {
                Message msg2(g_transID, memberNode->addr, CREATE, key, value, SECONDARY);
                emulNet->ENsend(&memberNode->addr, replicaNodes[1].getAddress(), msg2.toString());

                Message msg3(g_transID, memberNode->addr, CREATE, key, value, TERTIARY);
                emulNet->ENsend(&memberNode->addr, replicaNodes[2].getAddress(), msg3.toString());
            }

        //CASE TWO I AM THE SECONDARY
        else  if (replicaNodes[1].getAddress() == &(memberNode->addr)) {
                Message msg1(g_transID, memberNode->addr, CREATE, key, value, PRIMARY);
                emulNet->ENsend(&memberNode->addr, replicaNodes[0].getAddress(), msg1.toString());

                Message msg3(g_transID, memberNode->addr, CREATE, key, value, TERTIARY);
                emulNet->ENsend(&memberNode->addr, replicaNodes[2].getAddress(), msg3.toString());
        }

        //CASE 3 - I AM THE TERTIARY
        else if (replicaNodes[2].getAddress() == &(memberNode->addr)) {

                Message msg1(g_transID, memberNode->addr, CREATE, key, value, PRIMARY);
                emulNet->ENsend(&memberNode->addr, replicaNodes[0].getAddress(), msg1.toString());

                Message msg2(g_transID, memberNode->addr, CREATE, key, value, SECONDARY);
                emulNet->ENsend(&memberNode->addr, replicaNodes[1].getAddress(), msg2.toString());
        }

        //CASE 4 - ALL YOUR NODES (NOT) BELONG TO US
        else {           
                Message msg1 (g_transID, memberNode->addr, CREATE, key, value, PRIMARY);
                emulNet->ENsend(&memberNode->addr, replicaNodes[0].getAddress(), msg1.toString());

eplicaNodes[0].getAddress()
            }     Message msg2 (g_transID, memberNode->addr, CREATE, key, value, SECONDARY);
                emulNet->ENsend(&memberNode->addr, replicaNodes[1].getAddress(), msg2.toString());

                Message msg3 (g_transID, memberNode->addr, CREATE, key, value, TERTIARY);
                emulNet->ENsend(&memberNode->addr, replicaNodes[2].getAddress(), msg3.toString());
        }
    }
}
*/
/*
 * UTILITY FUNCTIONS
 *
*/


Address MP2Node::makeAddress(int id, short port) {
    Address addr;
    memcpy(&addr.addr[0], &id, sizeof(int));
    memcpy(&addr.addr[4], &port, sizeof(short));
    return addr;
}

int MP2Node::getAddressId(Address myAddress) {

    int id;
    memcpy(&id, &myAddress.addr, sizeof(int));
    return id;
}

short MP2Node::getAddressPort(Address myAddress) {
    short port;
    memcpy(&port, &myAddress.addr[4], sizeof(short));
    return port;
}
int MP2Node::getMyId() {

    return this->getAddressId(memberNode->addr);

}

short MP2Node::getMyPort() {

    return this->getAddressPort(memberNode->addr);
}


/************
 * FUNCTION NAME: addTransaction
 *
 * DESCRIPTION: utility function to add a transaction, and increment the Teransaction Counter
 *
*/
long MP2Node::addTransaction(string key, string value, MessageType type )  {
    //Set up the various maps to keep track of this

    g_transID++;
    Transaction t;
    t.complete=false;
    t.key = key;
    t.value = value;
    t.time = par->getcurrtime();
    t.type = type;
    t.count = 0;
    transactions.emplace(g_transID,t);
    return g_transID;
}


void MP2Node::trace(string function, long transaction, Address myAddress, string key, string value, string description) {

    string sMyAddress = myAddress.getAddress();

    string sTrace = "MP2Node, "+ function + ", " + sMyAddress + ", " + to_string(transaction) + ", " + key + ", " +value + ", " + description;

    cout <<   sTrace << endl;

    ofstream myfile;
    myfile.open ("log.txt",ios::app);
    myfile <<  sTrace  << endl;
    myfile.close();

}

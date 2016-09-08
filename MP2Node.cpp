/**********************************
 * FILE NAME: MP2Node.cpp
 *@author harish2
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
	initialized = 0;
	transactionID = g_transID;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool changed = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

	if(initialized == 0){
		ring = curMemList;
		initialized = 1;
	}


	//If the hashTable was empty, we do not need to stabalize anything
	if(!(ht->isEmpty())){

		if(curMemList.size() != ring.size() ){
			
			changed = true;
		}

		else{

            for(unsigned int i = 0; i<ring.size(); i++){

				//there was a change, set the flag to true
				if(curMemList[i].getHashCode() != ring[i].getHashCode()){

					changed = true;
					break;
				}

			}
		}		

	}


	//save the old neighbors so that they can be used in the stabalization protocol
	//Then call the stabilization protocol
	if(changed){

		ring = curMemList;
		OldhasMyReplicas = hasMyReplicas;
		OldhaveReplicasOf = haveReplicasOf;

		hasMyReplicas.clear();
		haveReplicasOf.clear();

		stabilizationProtocol();
	}




	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
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
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
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
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {

	 //Get the vector which containts the three nodes where the key will map to
	//Create a message and send it to the respective node

	 vector<Node> vec = findNodes(key);

	//New Primary Node
	Message messageP(transactionID, memberNode->addr, CREATE, key, value, PRIMARY);
	string toSendp = messageP.toString();
	emulNet->ENsend(&memberNode->addr, vec[0].getAddress(), toSendp);

	//new secondary node
	Message messageS(transactionID, memberNode->addr, CREATE, key, value, SECONDARY);
	string toSends = messageS.toString();
	emulNet->ENsend(&memberNode->addr, vec[1].getAddress(), toSends);


	//new tertiary node
	Message messageT(transactionID, memberNode->addr, CREATE, key, value, TERTIARY);
	string toSendt = messageT.toString();
	emulNet->ENsend(&memberNode->addr, vec[2].getAddress(), toSendt);
	
	//Set up the various maps to keep track of this

	clientCreateMap(key, value);


}

/*
* A function to set up the map for client create message
*/

void MP2Node::clientCreateMap(string key, string value){

	transComplete.emplace(transactionID, false);
	transKey.emplace(transactionID, key);
	transValue.emplace(transactionID, value);
	transTime.emplace(transactionID, par->getcurrtime());
	transType.emplace(transactionID, CREATE);
	transNum.emplace(transactionID, 0);
	
	incrementTransaction();
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */

	 vector<Node> vec = findNodes(key);

     for(unsigned int i = 0; i<vec.size(); i++){

	 	Message message(transactionID, memberNode->addr, READ, key);
 		string toSend = message.toString(); 
 		emulNet->ENsend(&memberNode->addr, vec[i].getAddress(), toSend);

	 }

	 //Set up the map values for the client read
	transComplete.emplace(transactionID, false);
	transKey.emplace(transactionID, key);
	transTime.emplace(transactionID, par->getcurrtime());
	transType.emplace(transactionID, READ);
	transNum.emplace(transactionID, 0);
	incrementTransaction();
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
	/*
	 * Implement this
	 */

	vector<Node> vec = findNodes(key);
	if(vec.size() == 0){
	 	log->logUpdateFail(&memberNode->addr, true, transactionID, key, value);
	 	return;
	 }

    for(unsigned int i = 0; i<vec.size(); i++){

	 	Message message(transactionID, memberNode->addr, UPDATE, key, value);
 		string toSend = message.toString(); 
 		emulNet->ENsend(&memberNode->addr, vec[i].getAddress(), toSend);

	 }


	//Set up the map for client update 

	transComplete.emplace(transactionID, false);
	transKey.emplace(transactionID, key);
	transValue.emplace(transactionID, value);
	transTime.emplace(transactionID, par->getcurrtime());
	transType.emplace(transactionID, UPDATE);
	transNum.emplace(transactionID, 0);
	incrementTransaction();


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
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */

	vector<Node> vec = findNodes(key);

    for(unsigned int i = 0; i<vec.size(); i++){

	 	Message message(transactionID, memberNode->addr, DELETE, key);
 		string toSend = message.toString(); 
 		emulNet->ENsend(&memberNode->addr, vec[i].getAddress(), toSend);


	 }

	 //Set up map for cleint Delete

	 clientDeleteMap(key);
}

/*
* Helper function to clear the map after client delete
*/
void MP2Node::clientDeleteMap(string key){
	transComplete.emplace(transactionID, false);
	transKey.emplace(transactionID, key);
	transTime.emplace(transactionID, par->getcurrtime());
	transType.emplace(transactionID, DELETE);
	transNum.emplace(transactionID, 0);
	incrementTransaction();
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {

	// Insert key, value, replicaType into the hash table

	 //Firstly check if the key
	 string read = ht->read(key);

	 //this key was not present, hence we can add it. 
	 if(read == ""){

	 	bool retVal = ht->create(key, value);

	 	//This is the primary replica, Update the neighbors accordingly
	 	if(replica == PRIMARY){
	 		createKeyValuePrimary(key);
	 		
	 	} //end of PRIMARY IF

	 	
	 	// you are a tertiary node, Update the neighbors accordinglt
	 	else if(replica == TERTIARY){

	 		createKeyValueTertiary(key);

	 	} //End of TERTIARY IF


	 	return retVal;

	 }
	 else
	 	return false;
	 
}

/*
* Create a primary replica helper function
*/ 
void MP2Node::createKeyValuePrimary(string key){

	vector<Node> vec = findNodes(key);

	 		bool insert = false;

	 		//Add the first neighbor if not already present
            for(unsigned int i=0; i<hasMyReplicas.size(); i++){
	 			if(hasMyReplicas[i].getHashCode() != vec[1].getHashCode()){
	 				insert = true;
	 			}
	 		}
	 		if(insert){
		 		hasMyReplicas.emplace_back(vec[1]);
	 		}


	 		insert = false;

	 		//Add the second neighbor if not already present
            for(unsigned int i=0; i<hasMyReplicas.size(); i++){
	 			if(hasMyReplicas[i].getHashCode() != vec[2].getHashCode()){
	 				insert = true;
	 			}
	 		}

	 		if(insert){
	 			hasMyReplicas.emplace_back(vec[2]);
	 		}

}

/*
* Create teritiary nd
*/

void MP2Node::createKeyValueTertiary(string key){

	 		vector<Node> vec = findNodes(key);

	 		bool insert = false;

	 		//Add the first neighbor if not already present
            for(unsigned int i=0; i<haveReplicasOf.size(); i++){
	 			if(haveReplicasOf[i].getHashCode() != vec[0].getHashCode()){
	 				insert = true;
	 			}
	 		}
	 		if(insert){
		 		haveReplicasOf.emplace_back(vec[1]);
	 		}

	 		//Add the second neighbor if not alrady present
	 		insert = false;
            for(unsigned int i=0; i<haveReplicasOf.size(); i++){
	 			if(haveReplicasOf[i].getHashCode() != vec[1].getHashCode()){
	 				insert = true;
	 			}
	 		}

	 		if(insert){
	 			haveReplicasOf.emplace_back(vec[2]);
	 		}

}


/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {

	// Read key from local hash table and return value

	 return ht->read(key);
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

	 string read = ht->read(key);

	 if( read == ""){
	 	return false;
	 }
	 else{

	 	return ht->update(key, value);
	 	
	 }

}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {

	// Delete the key from the local hash table

	 	return ht->deleteKey(key);
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
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

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

		 Message messageR(message);

		 //Get the type of message, address of the Node and the TrasanctionID
		 //From the message
		 MessageType type = messageR.type;
         //Address fromAddr =  messageR.fromAddr;
		 long transactionID = messageR.transID;

		 bool isFinished = transComplete.find(transactionID)->second;

		 //If this transaction has already been handled, then skip this message
		 if(isFinished){
		 	continue;
		 }

		 //Handle the CREATE message
		 if(type == CREATE){

		 	createMessageHandler(messageR);

		 } //End of CREATE MESSAGE

		 //Handle the READ message
		 else if(type == READ){

			readMessagehandler(messageR);

		 }//End of READ MESSAGE
		 
		 //Handle the UPDATE message
		 else if(type == UPDATE){

		 	updateMessagehandler(messageR);
		 	
		 }//End of UPDATE message

		 //Handle DELETE message
		 else if(type == DELETE){
		 	
		 	deleteMessagehandler(messageR);
		 	
		 }//End of DELETE message

		 //Handle a reply message
		 else if(type == REPLY){

		 	long transactionID = messageR.transID;

		 	// bool alreadyComplete = transComplete.find(transactionID)->second;
		 	// //if this transaction has been completed, then continue and ignore this
		 	// if(alreadyComplete){
		 	// 	continue;
		 	// }

		 	MessageType Replytype = transType.find(transactionID)->second;

		 	//REPLY of a CREATE
		 	if(Replytype == CREATE){

		 		createReplyMessageHandler(messageR, transactionID);

		 	}//End of REPLY of CREATE

		 	//REPLY of a UPDATE 
		 	else if(Replytype == UPDATE){

		 		updateReplyMessageHandler(messageR, transactionID);

		 	}//End of UPDATE REPLY


		 	//REPLY of a DELETE
		 	else if(Replytype == DELETE){

		 		deleteReplyMessageHandler(messageR, transactionID);

		 	}//END of DELETE REPLY

		 } //END of REPLY message

		 //Handle READREPLY
		 else if(type == READREPLY){

		 	readreplyMessageHandler(messageR, transactionID);

		 }//END of READREPLY

	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */

	 //Timeout on requests that you have been waiting too long on

	 transactionsTimeout();

}

/*
* A Helper function to clear all the maps for a given transactionID
*/
void MP2Node::clearMap(long transactionID){

	transComplete.emplace(transactionID, true);
	transKey.erase(transactionID);
	transType.erase(transactionID);
	transValue.erase(transactionID);
	transValue2.erase(transactionID);
	transNum.erase(transactionID);
}

/*
*Helper function to reject transactions which have times out
*/
void MP2Node::transactionsTimeout(){

	map<long, int>::iterator it = transTime.begin();

	while(it!= transTime.end()){

		if((par->getcurrtime() - it->second) > 10){

			long transactionID = it->first;

			MessageType type = transType.find(transactionID)->second;

			//The original message was a CREATE message
			//We will clear the associated elements from the map
			if(type == CREATE){
				string key = transKey.find(transactionID)->second;
				string value = transValue.find(transactionID)->second;

				log->logCreateFail(&memberNode->addr, true, transactionID, key, value);

				clearMap(transactionID);

			}
			//The original message was a READ message
			//We will clear the associated elements from the map
			else if(type == READ){

				string key = transKey.find(transactionID)->second;
				
				log->logReadFail(&memberNode->addr, true, transactionID, key);

				clearMap(transactionID);
			}
			//The original message was a DELETE message
			//We will clear the associated elements from the map
			else if(type == DELETE){

				string key = transKey.find(transactionID)->second;

				log->logDeleteFail(&memberNode->addr, true, transactionID, key);

				clearMap(transactionID);

			}
			//The original message was a UPDATE message
			//We will clear the associated elements from the map
			else if(type == UPDATE){

				string key = transKey.find(transactionID)->second;
				string value = transValue.find(transactionID)->second;

				log->logUpdateFail(&memberNode->addr, true, transactionID, key, value);

				clearMap(transactionID);

			}

			//erase the value currently at the iterator
			//Erase the time separately as we want make sure that the iterator is valid
			transTime.erase(it++);
		}
		//just iterate over simply as nothing was delete
		else{
			++it;
		}

	}

}

/*
* Helper Function for handling READREPLY messages
*/
void MP2Node::readreplyMessageHandler(Message messageR, long transactionID){


 	int totalSoFar = transNum.find(transactionID)->second;

 	//THis is the first message so far
 	// put the received value in a map and update the counr
 	if(totalSoFar == 0){
 		transNum.erase(transactionID);
 		transNum.emplace(transactionID, totalSoFar+1);

 		string value = messageR.value;
 		transValue.emplace(transactionID, value);
 	}
 	//this is the second message so far
 	else if(totalSoFar == 1){

 		string oldVal = transValue.find(transactionID)->second;
 		string newVal = messageR.value;
 		string key = transKey.find(transactionID)->second;

 		//The Quorom has reached as two nodes agree on a value and that value is not a falied value
 		//Log the message and clear the map and mark this transaction done
 		if(oldVal == newVal && oldVal != "_"){

 			log->logReadSuccess(&memberNode->addr, true, transactionID, key, newVal);

 			clearMap(transactionID);
 			transTime.erase(transactionID);


 		}
 		//Quoron has not been reached yet
 		else{

 			transValue2.emplace(transactionID, newVal);
 			transNum.erase(transactionID);
 			transNum.emplace(transactionID, totalSoFar+1);
 		}

 	}
 	//This is the third message
 	else if(totalSoFar == 2){

 		string oldVal = transValue.find(transactionID)->second;
 		string newVal = transValue2.find(transactionID)->second;

 		string myVal = messageR.value;
 		string key = transKey.find(transactionID)->second;

 		 //The Quorom has reached as two nodes agree on a value and that value is not a falied value
 		//Log the message and clear the map and mark this transaction done
 		if(myVal == oldVal && oldVal != "_"){
			log->logReadSuccess(&memberNode->addr, true, transactionID, key, oldVal);

 			clearMap(transactionID);

 			transNum.erase(transactionID);
 		}
 		 //The Quorom has reached as two nodes agree on a value and that value is not a falied value
 		//Log the message and clear the map and mark this transaction done
 		else if(myVal == newVal && newVal != "_"){
 			log->logReadSuccess(&memberNode->addr, true, transactionID, key, newVal);

 			clearMap(transactionID);
 			transNum.erase(transactionID);

 		}
 		//All the three nodes either failed or had different value, no quorom was reached
 		//This transaction failed, log the failure and clear the map
 		else{

 			log->logReadFail(&memberNode->addr, true, transactionID, key);

 			clearMap(transactionID);
 			transNum.erase(transactionID);

 		}


 	}
}


/*
* Helper Function for handling DELETE REPLY messages
*/
void MP2Node::deleteReplyMessageHandler(Message messageR, long transactionID){


	string key = transKey.find(transactionID)->second;
	bool retVal = messageR.success;

	//The reply was a success reply
	if(retVal){
		int totalSoFar = transNum.find(transactionID)->second;
		//We have received the second success message, quorum has been reached
		//Mark this transaction as done, and log success
		if(totalSoFar>=1){
			log->logDeleteSuccess(&memberNode->addr, true, transactionID, key);

			clearMap(transactionID);
			transTime.erase(transactionID);
		}
		//The quorum has not been reached yet
		else{
			transNum.erase(transactionID);
			transNum.emplace(transactionID, totalSoFar+1);
		}
	}
	//The reply was a filure
	else{
		int totalFail = transInvalid.find(transactionID)->second;
		//We have received the second failure message, quorum has been reached
		//Mark this transaction as done, and log failure
		if(totalFail>=1){
			log->logDeleteFail(&memberNode->addr, true, transactionID, key);

			clearMap(transactionID);
			transTime.erase(transactionID);
		}
		//The quorum has not been reached yet
		else{

			transInvalid.erase(transactionID);
			transInvalid.emplace(transactionID, totalFail+1);
		}
	}
}

/*
* Helper Function for handling DELETE REPLY messages
*/
void MP2Node::updateReplyMessageHandler(Message messageR, long transactionID){

	string key = transKey.find(transactionID)->second;
	string value = transValue.find(transactionID)->second;


	bool retVal = messageR.success;
	//The reply was a success reply
	if(retVal){
		int totalSoFar = transNum.find(transactionID)->second;
		//We have received the second success message, quorum has been reached
		//Mark this transaction as done, and log success
		if(totalSoFar>=1){
			log->logUpdateSuccess(&memberNode->addr, true, transactionID, key, value);

			clearMap(transactionID);
			transTime.erase(transactionID);
		}
		//The quorum has not been reached yet
		else{
			transNum.erase(transactionID);
			transNum.emplace(transactionID, totalSoFar+1);
		}
	}
	//The reply was a filure
	else{
		int totalFail = transInvalid.find(transactionID)->second;
		//We have received the second failure message, quorum has been reached
		//Mark this transaction as done, and log failure
		if(totalFail>=1){
			log->logUpdateFail(&memberNode->addr, true, transactionID, key, value);

			clearMap(transactionID);
			transTime.erase(transactionID);
		}
		//The quorum has not been reached yet
		else{

			transInvalid.erase(transactionID);
			transInvalid.emplace(transactionID, totalFail+1);
		}
	}

}


/*
* Helper Function for handling DELETE REPLY messages
*/
void MP2Node::createReplyMessageHandler(Message messageR, long transactionID){

	string key = transKey.find(transactionID)->second;
	string value = transValue.find(transactionID)->second;

	bool retVal = messageR.success;
	//The reply was a success
	if(retVal){
		int totalSoFar = transNum.find(transactionID)->second;
		//We have received the second success message, quorum has been reached
		//Mark this transaction as done, and log success
		if(totalSoFar>=1){
			log->logCreateSuccess(&memberNode->addr, true, transactionID, key, value);

			clearMap(transactionID);
			transTime.erase(transactionID);
		}
		//The quorum has not been reached yet
		else{
			transNum.erase(transactionID);
			transNum.emplace(transactionID, totalSoFar+1);
		}
	}
	//The reply was a filure
	else{
		int totalFail = transInvalid.find(transactionID)->second;
		//We have received the second failure message, quorum has been reached
		//Mark this transaction as done, and log failure
		if(totalFail>=1){
			log->logCreateFail(&memberNode->addr, true, transactionID, key, value);

			clearMap(transactionID);
			transTime.erase(transactionID);
		}
		//The quorum has not been reached yet
		else{

			transInvalid.erase(transactionID);
			transInvalid.emplace(transactionID, totalFail+1);
		}
	}
}


/*
* Helper Function for handling UPDATE messages
*/
void MP2Node::updateMessagehandler(Message messageR){

	//Get the message variables
	 Address fromAddr =  messageR.fromAddr;
	 long transactionID = messageR.transID;

 	string key = messageR.key;
 	string value = messageR.value;
 	ReplicaType replica = messageR.replica;

 	//update the key
 	bool retVal = updateKeyValue(key, value, replica);

 	//send the message reply
 	Message messageS(transactionID, memberNode->addr, REPLY, retVal);
	string toSendS = messageS.toString(); 
	emulNet->ENsend(&memberNode->addr, &fromAddr, toSendS);

	if(retVal){
		//success
		log->logUpdateSuccess(&memberNode->addr, false, transactionID, key, value);
 	}
 	else{
 		//failure
 		log->logUpdateFail(&memberNode->addr, false, transactionID, key, value);
 	}

}




/*
* Helper Function for handling DELETE messages
*/

void MP2Node::deleteMessagehandler(Message messageR){

	//Get the message variables
	 Address fromAddr =  messageR.fromAddr;
	 long transactionID = messageR.transID;

	string key = messageR.key;

 	bool retVal = deletekey(key);

 	Message messageS(transactionID, memberNode->addr, REPLY, retVal);
		string toSendS = messageS.toString(); 
		emulNet->ENsend(&memberNode->addr, &fromAddr, toSendS);

		if(retVal){
 		//success
 		log->logDeleteSuccess(&memberNode->addr, false, transactionID, key);
 	}
 	else{
 		//failure
 		log->logDeleteFail(&memberNode->addr, false, transactionID, key);
 	}

}
/*
* Helper Function for handling READ messages
*/
void MP2Node::readMessagehandler(Message messageR){


	//Get the message variables
	 Address fromAddr =  messageR.fromAddr;
	 long transactionID = messageR.transID;

	string key = messageR.key;
 	string read = readKey(key);


 	//The key was found, Log success and send the message
 	if(read != ""){
 		Message messageS(transactionID, memberNode->addr, read);
 		string toSendS = messageS.toString(); 
 		emulNet->ENsend(&memberNode->addr, &fromAddr, toSendS);
 		log->logReadSuccess(&memberNode->addr, false, transactionID, key, read);
 	}
 	//A key was not found, Log failure and send the message 
 	else{
 		Message messageS(transactionID, memberNode->addr, "_");
 		string toSendS = messageS.toString(); 
 		emulNet->ENsend(&memberNode->addr, &fromAddr, toSendS);
 		log->logReadFail(&memberNode->addr, false, transactionID, key);
 	}
}

/*
* Helper Function for handling CREATE messages
*/
void MP2Node::createMessageHandler(Message messageR){

	//Get the message variables
	 Address fromAddr =  messageR.fromAddr;
	 long transactionID = messageR.transID;


	string key = messageR.key;
 	string value = messageR.value;
	ReplicaType replica = messageR.replica;

	//Create a KeyValue
 	bool retVal = createKeyValue(key, value, replica);


 	//Create and send the message and then Log Success of
	Message messageS(transactionID, memberNode->addr, REPLY, retVal);
	string toSendS = messageS.toString(); 
	emulNet->ENsend(&memberNode->addr, &fromAddr, toSendS);

 	if(retVal){
 		//success
 		log->logCreateSuccess(&memberNode->addr, false, transactionID, key, value);
 	}
 	else{
 		//failure
 		log->logCreateFail(&memberNode->addr, false, transactionID, key, value);
 	}

}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
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
            for (unsigned int i=1; i<ring.size(); i++){
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
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */

	 map<string, string>::iterator it;

	 for(it = ht->hashTable.begin(); it != ht->hashTable.end(); it++){


	 	string key = it->first;
	 	string value = it->second;

	 	//Delete the values from the other nodes of whose replicas you have
	 	if(OldhaveReplicasOf.size()>0){

	 		Message message(transactionID, memberNode->addr, DELETE, it->first);
	 		string toSend = message.toString(); 
	 		emulNet->ENsend(&memberNode->addr, OldhaveReplicasOf[0].getAddress(), toSend);

	 		//Set up the Map
	 		clientDeleteMap(key);

	 	}
	 	//Delete the values in your neighbor
	 	if(OldhaveReplicasOf.size()>1){

	 		Message message(transactionID, memberNode->addr, DELETE, it->first);
	 		string toSend = message.toString(); 
	 		emulNet->ENsend(&memberNode->addr, OldhaveReplicasOf[1].getAddress(), toSend);

			clientDeleteMap(key);


	 	}

	 	//Delte the values from the other nodes who have your replica
	 	if(OldhasMyReplicas.size()>0){

	 		Message message(transactionID, memberNode->addr, DELETE, it->first);
	 		string toSend = message.toString(); 
	 		emulNet->ENsend(&memberNode->addr, OldhaveReplicasOf[0].getAddress(), toSend);

			clientDeleteMap(key);

	 	}
		//Delete the values in your neighbor
	 	if(OldhasMyReplicas.size()>1){

	 		Message message(transactionID, memberNode->addr, DELETE, it->first);
	 		string toSend = message.toString(); 
	 		emulNet->ENsend(&memberNode->addr, OldhaveReplicasOf[1].getAddress(), toSend);

			clientDeleteMap(key);

	 	}


	 	//Now find the new nodes where this key would be hashed to and add the key 
	 	//value to that node
		vector<Node> vec = findNodes(key);

		//New Primary Node
		Message messageP(transactionID, memberNode->addr, CREATE, key, value, PRIMARY);
		string toSendp = messageP.toString();
		emulNet->ENsend(&memberNode->addr, vec[0].getAddress(), toSendp);


		//new secondary Node
		Message messageS(transactionID, memberNode->addr, CREATE, key, value, SECONDARY);
		string toSends = messageS.toString();
		emulNet->ENsend(&memberNode->addr, vec[1].getAddress(), toSends);


		//new tertiary node
		Message messageT(transactionID, memberNode->addr, CREATE, key, value, TERTIARY);
		string toSendt = messageT.toString();
		emulNet->ENsend(&memberNode->addr, vec[2].getAddress(), toSendt);
		
		//create the map for this transaction
		clientCreateMap(key, value);


	 }

	ht->clear();

	
}

/**
*Increment the transactionID after each message
*/
void MP2Node::incrementTransaction(){
	g_transID++;
	transactionID = g_transID;
}

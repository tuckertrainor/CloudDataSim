/**
 * File: ContinuousThread.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A server thread to handle continuous proofs of authorization.
 * Extends the IncrementalThread class.
 */

/* TODO: pore through run(), make adjustments as necessary, will need to redo passQuery and join at minimum, last edit near R's passQuery
 * 
 */

import java.lang.Thread;
import java.net.Socket;
import java.net.ConnectException;
import java.io.*;
import java.util.*;

public class ContinuousThread extends IncrementalThread {
	
	/**
	 * Constructor that sets up the socket we'll chat over
	 *
	 * @param _socket - The socket passed in from the server
	 * @param _my_tm - The Transaction Manager that called the thread
	 */
	public ContinuousThread(Socket _socket, CloudServer _my_tm) {
		super(_socket, _my_tm);
	}
	
	/* View consistency
	 * 1.  get freshest policy off own server
	 * 2.  perform first operation, double check local policy, perform proof, perform operation, PASS,ACK or FAIL
	       how do we double check local policy on own server without involving PS?
	 * 3.  repeat if next operation on coordinator's server, else have participant join
	 
	 * 4.  have server join (and set up update condition)
	 * 5.  coordinator sends its txn policy version and operation to server
	 * 6.  server compares its policy to coordinator's, uses freshest for proof then runs operation, returns PASS, ACK, v. or FAIL, v.
	 
	 * 7.  coordinator receives PASS, ACK, v. or FAIL, v., compares v. to own version
	 * 8.  if v. is fresher, update txn policy version, run 2PV starting on own server
	 
	 * 9.  2PV: send txn policy version to all participants, along with 2PV msg
	 *	   just received freshest from server S, send it back to him anyway? eat cost of 2-way msg?
	 * 10. each participant checks txn policy version, if < v. then sets it to v. and re-runs auths
	 * 11. sends back PASS or FAIL along with v.
	 *     should we have a different response if policy was up to date?
	 * 12. coordinator gathers policy versions again, repeats 2PV if fresher is found
	 
	 * 13. continues until PTC
	 */
	
	/**
	 * run() is basically the main method of a thread. This thread
	 * simply reads Message objects off of the socket.
	 */
	public void run() {
		generator = new Random(new Date().getTime());
		
		PrintStream printStreamOriginal = System.out;
		if (!my_tm.verbose) {
			System.setOut(new PrintStream(new OutputStream() {
				public void close() {}
				public void flush() {}
				public void write(byte[] b) {}
				public void write(byte[] b, int off, int len) {}
				public void write(int b) {}
			}));
		}
		
		try {
			// Print incoming message
			System.out.println("** New connection from " + socket.getInetAddress() +
							   ":" + socket.getPort() + " **");
			
			// Set up I/O streams with the calling thread
			final ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
			final ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
			
			Message msg = null;
			Message resp = null;
			
			while (true) {
				// Loop to read messages
				String msgText = "ACK";
				// Read and print message
				msg = (Message)input.readObject();
				System.out.println("[" + socket.getInetAddress() +
								   ":" + socket.getPort() + "] " + msg.theMessage);
				
				if (msg.theMessage.equals("DONE")) {
					break;
				}
				else if (msg.theMessage.equals("KILL")) {
					my_tm.shutdownServer();
					break;
				}
				else if (msg.theMessage.indexOf("POLICYUPDATE") != -1) { // Policy update from Policy Server
					String msgSplit[] = msg.theMessage.split(" ");
					int update = Integer.parseInt(msgSplit[1]);
					// Check that we aren't going backwards in a race condition
					if (my_tm.getPolicy() < update) {
						my_tm.setPolicy(update);
					}
					latencySleep(); // Simulate latency
					output.writeObject(new Message(msgText)); // send ACK
					break;
				}
				else if (msg.theMessage.indexOf("PARAMETERS") != -1) { // Configuration change
					// PARAMETERS <PROOF> <VM> <PUSH>
					String msgSplit[] = msg.theMessage.split(" ");
					my_tm.proof = msgSplit[1];
					my_tm.validationMode = Integer.parseInt(msgSplit[2]);
					my_tm.policyPush = Integer.parseInt(msgSplit[3]);
					System.out.println("Server parameters updated: " + msg.theMessage);
					System.out.println("Proof: " + my_tm.proof);
					System.out.println("Validation mode: " + my_tm.validationMode);
					System.out.println("Policy push mode: " + my_tm.policyPush);
					// No artificial latency needed, send ACK
					output.writeObject(new Message(msgText));
					break;
				}
				
				// Separate queries
				String queryGroup[] = msg.theMessage.split(",");
				for (int i = 0; i < queryGroup.length; i++) {
					// Handle instructions
					String query[] = queryGroup[i].split(" ");
					if (query[0].equals("R")) { // READ
						// Check server number, perform query or pass on
						if (Integer.parseInt(query[2]) == my_tm.serverNumber) { // Perform query on this server
							// Check that if a fresh Policy version is needed
							// (e.g. if this query has been passed in) it is set
							if (transactionPolicyVersion == 0) {
								if (my_tm.validationMode == 1) {
									// Get freshest policy on local server
									transactionPolicyVersion = my_tm.getPolicy();
								}
								else { // my_tm.validationMode == 2
									// Get and set freshest global policy
									my_tm.setPolicy(my_tm.callPolicyServer());
									transactionPolicyVersion = my_tm.getPolicy();									
								}
								System.out.println("Transaction " + query[1] +
												   " Policy version set: " +
												   transactionPolicyVersion);
							}
							
							// Check transaction policy against server policy
							if (checkLocalAuth() == false) {
								msgText = "ABORT LOCAL_POLICY_FAIL";
								System.out.println("ABORT LOCAL_POLICY_FAIL: " +
												   "READ for txn " + query[1] +
												   " sequence " + query[3]);
							}
							else { // OK to read
								System.out.println("READ for txn " + query[1] +
												   " sequence " + query[3]);
								databaseRead();
								// Add policy version for passed query logging
								msgText += " " + transactionPolicyVersion;
								// Add to query log
								if (addToQueryLog(query, transactionPolicyVersion)) {
									System.out.println("Transaction " + query[1] +
													   " sequence " + query[3] +
													   " query logged.");
								}
								else {
									System.out.println("Error logging query.");
								}
							}
						}
						else { // Pass to server
							int passServer = Integer.parseInt(query[2]);
							int txnNum = Integer.parseInt(query[1]);
							// Check if server has participated yet
							if (!sockList.hasSocket(passServer)) {
								if (!join(passServer, txnNum)) {
									msgText = "FAIL during join(" + passServer +
									") for txn " + txnNum; 
								}
							}
							if (msgText.equals("ACK")) { // join() did not fail above
								/* View consistency:
								 * Send transactionPolicyVersion, operation
								 * Rec'v PASS, ACK, policy used or FAIL, policy used
								 */

								// The other server has joined - send passQuery message, parse response
								String response = passQuery(Integer.parseInt(query[2]), queryGroup[i]);
								// Expecting PASS ACK POLICY or FAIL POLICY
								System.out.println("Response to READ of transaction " + query[1] +
												   " sequence " + query[3] +
												   " to server " + query[2] +
												   ": " + response);
								String respSplit[] = response.split(" ");
								if (respSplit[0].equals("PASS") && respSplit[1].equals("ACK")) {
									// Check policy returned from server
									int returnPolicy = Integer.parseInt(respSplit[2]);
									if (returnPolicy > transactionPolicyVersion) {
										// Run 2PV
										if (run2PV(returnPolicy)) {
											System.out.println("2PV success: transaction " + query[1] +
															   " sequence " + query[3] +
															   " policy version " + returnPolicy);
										}
										else {
											msgText = "ABORT LOCAL_POLICY_FAIL_2PV";
											System.out.println("ABORT LOCAL_POLICY_FAIL_2PV: " +
															   " txn " + query[1] +
															   " sequence " + query[3] +
															   ", policy version " + returnPolicy);
										}
									}
								}
								else { // FAIL
									msgText = "ABORT LOCAL_POLICY_FAIL";
									System.out.println("ABORT LOCAL_POLICY_FAIL: " +
													   "READ for txn " + query[1] +
													   " sequence " + query[3] +
													   ", policy version " + respSplit[1]);
								}
							}
						}
					}
/* NEED TO EDIT FROM HERE */
					else if (query[0].equals("W")) { // WRITE
						// Check server number, perform query or pass on
						if (Integer.parseInt(query[2]) == my_tm.serverNumber) { // Perform query on this server
							// Check that if a fresh Policy version is needed
							// (e.g. if this query has been passed in) it is set
							if (transactionPolicyVersion == 0) {
								if (my_tm.validationMode == 1) {
									// Get freshest policy on local server
									transactionPolicyVersion = my_tm.getPolicy();
								}
								else { // my_tm.validationMode == 2
									// Get and set freshest global policy
									my_tm.setPolicy(my_tm.callPolicyServer());
									transactionPolicyVersion = my_tm.getPolicy();									
								}
								System.out.println("Transaction " + query[1] +
												   " Policy version set: " +
												   transactionPolicyVersion);
							}
							
							// Check transaction policy against server policy
							if (checkLocalAuth() == false) {
								msgText = "ABORT LOCAL_POLICY_FAIL";
								System.out.println("ABORT LOCAL_POLICY_FAIL: " +
												   "WRITE for txn " + query[1] +
												   " sequence " + query[3]);
							}
							else { // OK to write
								System.out.println("WRITE for txn " + query[1] +
												   " sequence " + query[3]);
								databaseWrite();
								// Add policy version for passed query logging
								msgText += " " + transactionPolicyVersion;
								// Add to query log
								if (addToQueryLog(query, transactionPolicyVersion)) {
									System.out.println("Transaction " + query[1] +
													   " sequence " + query[3] +
													   " query logged.");
								}
								else {
									System.out.println("Error logging query.");
								}
							}
						}
						else { // Pass to server
							int passServer = Integer.parseInt(query[2]);
							int txnNum = Integer.parseInt(query[1]);
							// Check if server has participated yet
							if (!sockList.hasSocket(passServer)) {
								if (!join(passServer, txnNum)) {
									msgText = "FAIL during join(" + passServer +
									") for txn " + txnNum; 
								}
								else {
									// The other server has joined, so now run
									// a transaction consistency check
									if (checkTxnConsistency() == false) {
										msgText = "ABORT TXN_CONSISTENCY_FAIL";
										System.out.println("ABORT TXN_CONSISTENCY_FAIL: " +
														   "WRITE for txn " + query[1] +
														   " sequence " + query[3]);
									}
									else {
										// Consistency confirmed, pass operation
										System.out.println("*** Txn consistency validated for sequence " + query[3]);
										System.out.println("Pass WRITE of transaction " + query[1] +
														   " sequence " + query[3] +
														   " to server " + query[2]);
										msgText = passQuery(Integer.parseInt(query[2]), queryGroup[i]);
										System.out.println("Response to WRITE of transaction " + query[1] +
														   " sequence " + query[3] +
														   " to server " + query[2] +
														   ": " + msgText);
									}
								}
							}
							// Server has previously joined, pass operation
							else {
								msgText = passQuery(Integer.parseInt(query[2]), queryGroup[i]);
								System.out.println("Response to WRITE of transaction " + query[1] +
												   " sequence " + query[3] +
												   " to server " + query[2] +
												   ": " + msgText);
							}
						}
					}
					else if (query[0].equals("PASSR")) { // Passed read operation
						// Check transaction policy against server policy
						if (checkLocalAuth() == false) {
							msgText = "ABORT LOCAL_POLICY_FAIL";
							System.out.println("ABORT LOCAL_POLICY_FAIL: " +
											   "READ for txn " + query[1] +
											   " sequence " + query[3]);
						}
						else { // OK to read
							System.out.println("READ for txn " + query[1] +
											   " sequence " + query[3]);
							databaseRead();
							// Add policy version for passed query logging
							msgText += " " + transactionPolicyVersion;
							// Add to query log
							if (addToQueryLog(query, transactionPolicyVersion)) {
								System.out.println("Transaction " + query[1] +
												   " sequence " + query[3] +
												   " query logged.");
							}
							else {
								System.out.println("Error logging query.");
							}
						}
					}
					else if (query[0].equals("PASSW")) { // Passed write operation
						// Check transaction policy against server policy
						if (checkLocalAuth() == false) {
							msgText = "ABORT LOCAL_POLICY_FAIL";
							System.out.println("ABORT LOCAL_POLICY_FAIL: " +
											   "WRITE for txn " + query[1] +
											   " sequence " + query[3]);
						}
						else { // OK to write
							System.out.println("WRITE for txn " + query[1] +
											   " sequence " + query[3]);
							databaseWrite();
							// Add policy version for passed query logging
							msgText += " " + transactionPolicyVersion;
							// Add to query log
							if (addToQueryLog(query, transactionPolicyVersion)) {
								System.out.println("Transaction " + query[1] +
												   " sequence " + query[3] +
												   " query logged.");
							}
							else {
								System.out.println("Error logging query.");
							}
						}
					}
					else if (query[0].equals("JOIN")) {
						// Coordinator is requesting server to join txn,
						// set the transaction policy version for this server
						if (transactionPolicyVersion == 0) {
							if (my_tm.validationMode >= 0 && my_tm.validationMode <= 2) {
								// Get policy from the server
								transactionPolicyVersion = my_tm.getPolicy();
							}
							else { // Get and set freshest global policy
								my_tm.setPolicy(my_tm.callPolicyServer());
								transactionPolicyVersion = my_tm.getPolicy();									
							}
							System.out.println("Transaction " + query[1] +
											   " Policy version set: " +
											   transactionPolicyVersion);
						}
						if (transactionPolicyVersion < 1) {
							msgText = "JOIN_FAIL"; // Error message if policy is not properly set
						}
					}
					else if (query[0].equals("VERSION")) { // Coordinator is requesting policy version
						msgText = "VERSION " + transactionPolicyVersion;
					}
					else if (query[0].equals("PTC")) { // Prepare-to-Commit
						msgText = prepareToCommit(Integer.parseInt(query[1]));
					}
					else if (query[0].equals("C")) { // COMMIT
						System.out.println("COMMIT phase - transaction " + query[1]);
						// Force global update if necessary
						if (my_tm.policyPush == 5) {
							forcePolicyUpdate(3);
						}
						// Begin 2PC/2PV methods
						msgText = coordinatorCommit();
						System.out.println("Status of 2PC/2PV of transaction " + query[1] +
										   ": " + msgText);
					}
					else if (query[0].equals("RSERV")) { // Random server for policy pushing
						randomServer = Integer.parseInt(query[1]);
					}
					else if (query[0].equals("S")) { // Sleep for debugging
						Thread.sleep(Integer.parseInt(query[1]));
					}
					else if (query[0].toUpperCase().equals("EXIT")) { // end of transaction
						// send exit flag to RobotThread
						msgText = "FIN";
						if (!my_tm.threadSleep) { // append total sleep time to message
							msgText += " " + totalSleepTime;
						}
					}
				}
				latencySleep(); // Simulate latency to RobotThread
				// ACK completion of this query group to RobotThread
				output.writeObject(new Message(msgText));
			}
			// Close any SocketGroup connection
			if (sockList.size() > 0) {
				int serverNum;
				for (Enumeration<Integer> socketList = sockList.keys(); socketList.hasMoreElements();) {
					msg = new Message("DONE");
					serverNum = socketList.nextElement();
					latencySleep(); // Simulate latency
					sockList.get(serverNum).output.writeObject(msg);
					sockList.get(serverNum).socket.close();
				}
			}
			
			// Close and cleanup
			System.out.println("** Closing connection with " + socket.getInetAddress() +
							   ":" + socket.getPort() + " **");
			socket.close();
		}
		catch(Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
		System.out.flush();
		System.setOut(printStreamOriginal);
	}
	
	/**
	 * Passes a query to other specified server
	 *
	 * @param otherServer - The number of the server to pass to
	 * @param query - The query that must be performed on another server
	 *
	 * @return String - the response from the other server
	 */
	public String passQuery(int otherServer, String query) {
		/* View consistency:
		 * Send transactionPolicyVersion, operation
		 * Rec'v PASS, ACK, policy used or FAIL, policy used
		 */
		Message msg = null;
		
		try {
			
			// Add "PASS" to beginning of query (so we have PASSR or PASSW)
			// and the txn policy version
			msg = new Message("PASS" + query + " " + transactionPolicyVersion);
			// Send query
			latencySleep(); // Simulate latency to other server
			sockList.get(otherServer).output.writeObject(msg);
			msg = (Message)sockList.get(otherServer).input.readObject();
			System.out.println("Server " + otherServer +
							   " says: " + msg.theMessage +
							   " for passed query " + query);
			// else it is an ABORT, no need to log, will be handled by RobotThread
			return msg.theMessage;
		}
		catch (ConnectException ce) {
			System.err.println(ce.getMessage() +
							   ": Check server address and port number.");
			ce.printStackTrace(System.err);
		}
		catch (Exception e) {
			System.err.println("Error during passQuery(): " + e.getMessage());
			e.printStackTrace(System.err);
		}
		return "FAIL";
	}
	
	/**
	 * Performs the 2PV algorithm with the servers participating in the
	 * transaction.
	 *
	 * @param int - the policy version to perform 2PV with
	 *
	 * @return boolean - the result of the 2PV process
	 */
	public boolean run2PV(int policyVersion) {
		return true;
	}
	
	/**
	 * Performs the 2PVC algorithm with the servers participating in the
	 * transaction.
	 *
	 * @param int - the policy version to perform 2PVC with
	 *
	 * @return String - the result of the 2PVC process
	 */
	public String run2PVC(int policyVersion) {
		return "run2PVC stub";
	}
	
}
/**
 * File: IncrementalThread.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A server thread to handle incremental punctual proofs of authorization.
 * Extends the PunctualThread class.
 */

import java.lang.Thread;
import java.net.Socket;
import java.net.ConnectException;
import java.io.*;
import java.util.*;

public class IncrementalThread extends PunctualThread {
	
	/**
	 * Constructor that sets up the socket we'll chat over
	 *
	 * @param _socket - The socket passed in from the server
	 * @param _my_tm - The Transaction Manager that called the thread
	 */
	public IncrementalThread(Socket _socket, CloudServer _my_tm) {
		super(_socket, _my_tm);
	}
	
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
								if (my_tm.validationMode >= 0 && my_tm.validationMode <= 2) {
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
							
							// Check view/global consistency
							if (checkTxnConsistency(my_tm.validationMode) == false) {
								msgText = "ABORT TXN_CONSISTENCY_FAIL";
								System.out.println("ABORT TXN_CONSISTENCY_FAIL: " +
												   "READ for txn " + query[1] +
												   " sequence " + query[3]);
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
							System.out.println("Pass READ of transaction " + query[1] +
											   " sequence " + query[3] +
											   " to server " + query[2]);
							msgText = passQuery(Integer.parseInt(query[2]), queryGroup[i]);
							System.out.println("Response to READ of transaction " + query[1] +
											   " sequence " + query[3] +
											   " to server " + query[2] +
											   ": " + msgText);
						}
					}
					else if (query[0].equals("W")) { // WRITE
						// Check server number, perform query or pass on
						if (Integer.parseInt(query[2]) == my_tm.serverNumber) { // Perform query on this server
							// Check that if a fresh Policy version is needed, it is gotten
							if (transactionPolicyVersion == 0) {
								if (my_tm.validationMode >= 0 && my_tm.validationMode <= 2) {
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
							
							// Check view/global consistency
							if (checkTxnConsistency(my_tm.validationMode) == false) {
								msgText = "ABORT TXN_CONSISTENCY_FAIL";
								System.out.println("ABORT TXN_CONSISTENCY_FAIL: " +
												   "WRITE for txn " + query[1] +
												   " sequence " + query[3]);
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
					else if (query[0].equals("RUNAUTHS")) {
						// Run any necessary re-authorizations on queries
						int version = Integer.parseInt(query[1]);
						System.out.println("Running auth. on transaction " +
										   queryLog.get(0).getTransaction() + 
										   " queries using policy version " +
										   version);
						msgText = "TRUE";
						for (int j = 0; j < queryLog.size(); j++) {
							// If policy used for proof during transaction differs
							if (queryLog.get(j).getPolicy() != version) {
								if (!checkLocalAuth()) {
									System.out.println("Authorization of " + queryLog.get(j).getQueryType() +
													   " for txn " + queryLog.get(j).getTransaction() +
													   ", seq " + queryLog.get(j).getSequence() +
													   " with policy v. " + version +
													   " (was v. " + queryLog.get(j).getPolicy() +
													   "): FAIL");
									msgText = "FALSE";
									break;
								}
								else {
									System.out.println("Authorization of " + queryLog.get(j).getQueryType() +
													   " for txn " + queryLog.get(j).getTransaction() +
													   ", seq " + queryLog.get(j).getSequence() +
													   " with policy v. " + version +
													   " (was v. " + queryLog.get(j).getPolicy() +
													   "): PASS");
									queryLog.get(j).setPolicy(version); // Update policy in log
								}
							}
							else { // Output message of same policy
								System.out.println("Authorization of " + queryLog.get(j).getQueryType() +
												   " for txn " + queryLog.get(j).getTransaction() +
												   ", seq " + queryLog.get(j).getSequence() +
												   " with policy v. " + version +
												   ": ALREADY DONE");
							}
						}
					}
					else if (query[0].equals("VERSION")) { // Coordinator is requesting policy version
						msgText = "VERSION " + transactionPolicyVersion;
					}
					else if (query[0].equals("PTC")) { // Prepare-to-Commit
						if (my_tm.validationMode >= 0 && my_tm.validationMode <= 2) {
							msgText = prepareToCommit(0); // No global version
						}
						else { // Uses a global version, pass to method
							msgText = prepareToCommit(Integer.parseInt(query[1]));
						}
					}
					else if (query[0].equals("C")) { // COMMIT
						System.out.println("COMMIT phase - transaction " + query[1]);
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
	 * @return String - the ACK/ABORT from the other server
	 */
	public String passQuery(int otherServer, String query) {
		String server = my_tm.serverList.get(otherServer).getAddress();
		int port = my_tm.serverList.get(otherServer).getPort();
		Message msg = null;
		try {
			// Check SocketList for an existing socket, else create and add new
			if (!sockList.hasSocket(otherServer)) {
				// Create new socket, add it to SocketGroup
				System.out.println("Connecting to " + server +
								   " on port " + port);
				Socket sock = new Socket(server, port);
				sockList.addSocketObj(otherServer, new SocketObject(sock,
																	new ObjectOutputStream(sock.getOutputStream()),	
																	new ObjectInputStream(sock.getInputStream())));
				// Push policy updates as necessary
				if (!hasUpdated) {
					if (my_tm.validationMode == 1 || my_tm.validationMode == 3) {
						// We can update at the addition of any participant
						forcePolicyUpdate(my_tm.policyPush);
						hasUpdated = true; // This only needs to be done once
					}
					else if (my_tm.validationMode == 2 || my_tm.validationMode == 4) {
						// We want to randomize when the policy update is pushed
						if (otherServer == randomServer) { // Do if random server is picked
							forcePolicyUpdate(my_tm.policyPush);
							hasUpdated = true; // This only needs to be done once	
						}
					}
				}
			}

			// Add "PASS" to beginning of query (so we have PASSR or PASSW)
			msg = new Message("PASS" + query);
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

	public boolean checkTxnConsistency(int mode) {
		if (mode == 0) {
			// What is 2PC for Incremental Punctual?
			return true;
		}
		else {
			// View consistency - call all participants, see if versions match
			// Call all participants, send PTC and global version
			if (sockList.size() > 0) {
				int serverNum;
				Message msg = null;
				for (Enumeration<Integer> socketList = sockList.keys(); socketList.hasMoreElements();) {
					serverNum = socketList.nextElement();
					if (serverNum != 0) { // Don't call the Policy server
						try {
							System.out.println("Asking server " + serverNum +
											   " for txn policy version.");
							msg = new Message("VERSION");
							latencySleep(); // Simulate latency
							// Send
							sockList.get(serverNum).output.writeObject(msg);
							// Rec'v
							msg = (Message)sockList.get(serverNum).input.readObject();
							// Check response
							String msgSplit[] = msg.theMessage.split(" ");
							System.out.println("Server " + serverNum +
											   " is using txn policy version " +
											   Integer.parseInt(msgSplit[1]));
							if (Integer.parseInt(msgSplit[1]) != transactionPolicyVersion) {
								return false;
							}
						}
						catch (Exception e) {
							System.err.println("checkTxnConsistency() Error: " + e.getMessage());
							e.printStackTrace(System.err);
						}
					}
				}
			}
			
			return true;
		}
	}
}
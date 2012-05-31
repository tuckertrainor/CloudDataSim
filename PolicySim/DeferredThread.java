/**
 * File: DeferredThread.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A server thread to handle deferred proofs of authorization. Extends the
 * WorkerThread base class.
 */

import java.lang.Thread;
import java.net.Socket;
import java.net.ConnectException;
import java.io.*;
import java.util.*;

public class DeferredThread extends WorkerThread {
	
	/**
	 * Constructor that sets up the socket we'll chat over
	 *
	 * @param _socket - The socket passed in from the server
	 * @param _my_tm - The Transaction Manager that called the thread
	 */
	public DeferredThread(Socket _socket, CloudServer _my_tm) {
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
								transactionPolicyVersion = my_tm.getPolicy();
								System.out.println("Transaction " + query[1] +
												   " Policy version set: " +
												   transactionPolicyVersion);
							}
							
							// No check for local authorization, so OK to write
							System.out.println("READ for transaction " + query[1] +
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
								transactionPolicyVersion = my_tm.getPolicy();
								System.out.println("Transaction " + query[1] +
												   " Policy version set: " +
												   transactionPolicyVersion);
							}
							
							// No check for local authorization, so OK to write
							System.out.println("WRITE for transaction " + query[1] +
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
					else if (query[0].equals("RUNAUTHS")) { // Run authorizations on all queries
						int version = Integer.parseInt(query[1]);
						System.out.println("Running auth. on transaction " +
										   queryLog.get(0).getTransaction() + 
										   " queries using policy version " +
										   version);
						msgText = "TRUE";
						for (int j = 0; j < queryLog.size(); j++) {
							if (!checkLocalAuth()) {
								System.out.println("Authorization of " + queryLog.get(j).getQueryType() +
												   " for transaction " + queryLog.get(j).getTransaction() +
												   ", sequence " + queryLog.get(j).getSequence() +
												   " with policy v. " + version +
												   ": FAIL");
								msgText = "FALSE";
								break;
							}
							else {
								System.out.println("Authorization of " + queryLog.get(j).getQueryType() +
												   " for transaction " + queryLog.get(j).getTransaction() +
												   ", sequence " + queryLog.get(j).getSequence() +
												   " with policy v. " + version +
												   ": PASS");
							}
						}
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
						// Perform any forced policy updating for global
						if (my_tm.validationMode == 3 || my_tm.validationMode == 4) {
							forcePolicyUpdate(my_tm.policyPush);
						}
						// Begin 2PC/2PV methods
						msgText = coordinatorCommit();
						System.out.println("Status of 2PC/2PV of transaction " + query[1] +
										   ": " + msgText);
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
}

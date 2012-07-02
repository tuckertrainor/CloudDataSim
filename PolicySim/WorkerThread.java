/**
 * File: WorkerThread.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * Based on original code "EchoThread.java" by Adam J. Lee (adamlee@cs.pitt.edu) 
 *
 * A simple server thread. This class just echoes the messages sent
 * over the socket until the socket is closed.
 */

import java.lang.Thread;
import java.net.Socket;
import java.net.ConnectException;
import java.io.*;
import java.util.*;

public class WorkerThread extends Thread {
    public final Socket socket; // The socket that we'll be talking over
	public CloudServer my_tm; // The Transaction Manager that called the thread
	public SocketList sockList = new SocketList();
	public ArrayList<QueryRecord> queryLog = new ArrayList<QueryRecord>();
	public int transactionPolicyVersion = 0;
	public int totalSleepTime = 0; // used if my_tm.threadSleep == false
	public Random generator;
	public boolean hasUpdated = false;

	/**
	 * Constructor that sets up the socket we'll chat over
	 *
	 * @param _socket - The socket passed in from the server
	 * @param _my_tm - The Transaction Manager that called the thread
	 */
	public WorkerThread(Socket _socket, CloudServer _my_tm) {
		socket = _socket;
		my_tm = _my_tm;
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
				// If pushing updates for view consistency testing, do it now
				if (!hasUpdated && (my_tm.validationMode == 1 || my_tm.validationMode == 2)) {
					forcePolicyUpdate(my_tm.policyPush);
					hasUpdated = true; // This only needs to be done once
				}
			}

			// Send query
			msg = new Message(query);
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
	
	public boolean addToQueryLog(String query[], int policyVersion) {
		try {
			QueryRecord item = new QueryRecord(query[0],
											   Integer.parseInt(query[1]),
											   Integer.parseInt(query[2]),
											   Integer.parseInt(query[3]),
											   policyVersion);
			queryLog.add(item);
			return true;
		}
		catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
		return false;
	}
	
	/**
	 * When the coordinator receives a request to COMMIT, it directs the flow
	 * of the transaction to either a view consistency check or a global
	 * consistency check.
	 *
	 * @return String - the result of the 2PV check, either COMMIT or ABORT
	 */
	public String coordinatorCommit() {
		String commitStatus = "COMMIT";
		
		// Call each participating server with a PTC message		
		if (my_tm.validationMode >= 0 && my_tm.validationMode <= 2) {
			// View consistency checks
			commitStatus = viewConsistencyCheck();
		}
		else if (my_tm.validationMode == 3 || my_tm.validationMode == 4) {
			// Global consistency checks
			commitStatus = globalConsistencyCheck();
		}
		else {
			commitStatus = "ABORT UNKNOWN_MODE";
		}
		
		return commitStatus;
	}
	
	/**
	 * The prepare-to-commit method that is invoked when participating servers
	 * received the PTC call from the coordinator
	 *
	 * @param globalVersion - used for global consistency check
	 * @return boolean
	 */
	public String prepareToCommit(int globalVersion) {
		// Receive PTC message, handle options
		if (my_tm.validationMode == 0) { // 2PC only
			// Return integrity status
			if (integrityCheck()) {
				return "YES";
			}
			else {
				return "NO";
			}
		}
		else if (my_tm.validationMode == 1 || my_tm.validationMode == 2) {
			// 1. Rec'v PTC, request for policy version
			//    Return integrity status (YES/NO), Policy version
			if (integrityCheck()) {
				return "YES " + transactionPolicyVersion;
			}
			else {
				return "NO";
			}
		}
		else if (my_tm.validationMode == 3) {
			// Check global master policy version against transaction version
			if (globalVersion == transactionPolicyVersion) {
				// Perform integrity check
				if (integrityCheck()) {
					// Run local authorizations
					System.out.println("Running auth. on transaction " +
									   queryLog.get(0).getTransaction() + 
									   " queries using policy version " +
									   transactionPolicyVersion);
					for (int j = 0; j < queryLog.size(); j++) {
						if (!checkLocalAuth()) {
							System.out.println("Authorization of " + queryLog.get(j).getQueryType() +
											   " for transaction " + queryLog.get(j).getTransaction() +
											   ", sequence " + queryLog.get(j).getSequence() +
											   " with policy v. " + transactionPolicyVersion +
											   ": FAIL");
							return "YES FALSE"; // (authorization failed)
						}
						else {
							System.out.println("Authorization of " + queryLog.get(j).getQueryType() +
											   " for transaction " + queryLog.get(j).getTransaction() +
											   ", sequence " + queryLog.get(j).getSequence() +
											   " with policy v. " + transactionPolicyVersion +
											   ": PASS");
						}
					}
					return "YES TRUE"; // (integrity and authorizations pass)
				}
				else {
					return "NO FALSE"; // (integrity fail)
				}
			}
			else {
				return "YES FALSE"; // (policy inequality)
			}
		}
		else { // (my_tm.validationMode == 4)
			// Check global master policy version against transaction version
			if (globalVersion != transactionPolicyVersion) {
				// Have server get global version from the policy server
				int calledGlobal = my_tm.callPolicyServer();
				// Check version for possible race condition
				if (calledGlobal > globalVersion) {
					calledGlobal = globalVersion;
				}
				// Perform integrity check
				if (integrityCheck()) {
					// Run local authorizations
					System.out.println("Running auth. on transaction " +
									   queryLog.get(0).getTransaction() + 
									   " queries using policy version " +
									   calledGlobal);
					for (int j = 0; j < queryLog.size(); j++) {
						if (!checkLocalAuth()) {
							System.out.println("Authorization of " + queryLog.get(j).getQueryType() +
											   " for transaction " + queryLog.get(j).getTransaction() +
											   ", sequence " + queryLog.get(j).getSequence() +
											   " with policy v. " + calledGlobal +
											   ": FAIL");
							return "YES FALSE"; // (authorization failed)
						}
						else {
							System.out.println("Authorization of " + queryLog.get(j).getQueryType() +
											   " for transaction " + queryLog.get(j).getTransaction() +
											   ", sequence " + queryLog.get(j).getSequence() +
											   " with policy v. " + calledGlobal +
											   ": PASS");
						}
					}
					return "YES TRUE"; // (integrity and authorizations pass)
				}
				else {
					return "NO FALSE"; // (integrity fail)
				}
			}			
			else { // (globalVersion == transactionPolicyVersion) 
				// Perform integrity check
				if (integrityCheck()) {
					// Run local authorizations
					System.out.println("Running auth. on transaction " +
									   queryLog.get(0).getTransaction() + 
									   " queries using policy version " +
									   transactionPolicyVersion);
					for (int j = 0; j < queryLog.size(); j++) {
						if (!checkLocalAuth()) {
							System.out.println("Authorization of " + queryLog.get(j).getQueryType() +
											   " for transaction " + queryLog.get(j).getTransaction() +
											   ", sequence " + queryLog.get(j).getSequence() +
											   " with policy v. " + transactionPolicyVersion +
											   ": FAIL");
							return "YES FALSE"; // (authorization failed)
						}
						else {
							System.out.println("Authorization of " + queryLog.get(j).getQueryType() +
											   " for transaction " + queryLog.get(j).getTransaction() +
											   ", sequence " + queryLog.get(j).getSequence() +
											   " with policy v. " + transactionPolicyVersion +
											   ": PASS");
						}
					}
					return "YES TRUE"; // (integrity and authorizations pass)
				}
				else {
					return "NO FALSE"; // (integrity fail)
				}
			}
		}
	}

	/**
	 * Handles the 2PV view consistency check. Calls each participant with the
	 * PTC command, receives back their policy versions, and determines whether
	 * or not to run proofs of authorization.
	 *
	 * @return String - COMMIT or ABORT under view consistency
	 */
	public String viewConsistencyCheck() {
		String status = "COMMIT";
		Message msg = null;
		ArrayList<Integer> versions = new ArrayList<Integer>();

		// Check coordinator's integrity
		if (!integrityCheck()) {
			return "ABORT PTC_RESPONSE_NO";
		}

		// Add coordinator's policy version to ArrayList
		versions.add(transactionPolicyVersion);
		// Call all participants, send PTC and gather policy versions
		if (sockList.size() > 0) {
			int serverNum;
			for (Enumeration<Integer> socketList = sockList.keys(); socketList.hasMoreElements();) {
				serverNum = socketList.nextElement();
				if (serverNum != 0) { // Don't call the Policy server
					try {
						msg = new Message("PTC");
						latencySleep(); // Simulate latency
						// Send
						sockList.get(serverNum).output.writeObject(msg);
						// Rec'v
						msg = (Message)sockList.get(serverNum).input.readObject();
						// Check response, add policy version to ArrayList
						if (msg.theMessage.indexOf("YES") != -1) {
							if (my_tm.validationMode != 0) { // Not 2PC only
								String msgSplit[] = msg.theMessage.split(" ");
								versions.add(Integer.parseInt(msgSplit[1]));
							}
						}
						else { // ABORT - someone responded with a NO
							return "ABORT PTC_RESPONSE_NO";
						}
					}
					catch (Exception e) {
						System.err.println("Policy Check Error: " + e.getMessage());
						e.printStackTrace(System.err);
					}
				}
			}
		}
		
		// If 2PC only, no need to compare policy versions or run auths
		if (my_tm.validationMode == 0) {
			return status;
		}
		
		// Turn ArrayList into an array of ints, sort and compare versions
		Integer versionArray[] = new Integer[versions.size()];
		versionArray = versions.toArray(versionArray);
		// Sort array, compare first value with last
		Arrays.sort(versionArray);
		if (versionArray[0] == versionArray[versionArray.length - 1]) {
			// Policy versions match across servers - run authorizations
			status = runAuths((int)versionArray[0]);
		}
		else { // Handle inequality
			if (my_tm.validationMode == 1) { // ABORT
				status = "ABORT VIEW_CONSISTENCY_FAIL";
			}
			else { // Find common policy and run authorizations with it
				// For simplicity, use minimum of versions as common policy
				status = runAuths((int)versionArray[0]);
			}
		}
		
		return status;
	}

	/**
	 * Handles the 2PV global consistency check. Sends each participant the PTC
	 * command and the global master policy version. If all participants are
	 * using the global version, then authorizations can be performed. Otherwise
	 * a decision is made whether to allow calls to the policy server to refresh
	 * or to ABORT.
	 *
	 * @return String - COMMIT or ABORT
	 */
	public String globalConsistencyCheck() {
		String status = "COMMIT";
		Message msg = null;
		
		// Have coordinator's server call the policy server and retrieve the
		// current global master policy version
		int globalVersion = my_tm.callPolicyServer();
		
		// Check coordinator for its version
		if (transactionPolicyVersion != globalVersion) {
			if (my_tm.validationMode == 3) {
				return "ABORT GLOBAL_CONSISTENCY_FAIL";
			}
			else { // mode == 4
				// Check coordinator's integrity
				if (!integrityCheck()) {
					return "ABORT PTC_RESPONSE_NO";
				}
				
				// Run auths using global version
				for (int j = 0; j < queryLog.size(); j++) {
					if (!checkLocalAuth()) {
						System.out.println("Authorization of " + queryLog.get(j).getQueryType() +
										   " for transaction " + queryLog.get(j).getTransaction() +
										   ", sequence " + queryLog.get(j).getSequence() +
										   " with policy v. " + globalVersion +
										   ": FAIL");
						return "ABORT PTC_RESPONSE_FALSE";
					}
					else {
						System.out.println("Authorization of " + queryLog.get(j).getQueryType() +
										   " for transaction " + queryLog.get(j).getTransaction() +
										   ", sequence " + queryLog.get(j).getSequence() +
										   " with policy v. " + globalVersion +
										   ": PASS");
						queryLog.get(j).setPolicy(globalVersion); // Update policy in log
					}
				}
			}
		}
		
		// Call all participants, send PTC and global version
		if (sockList.size() > 0) {
			int serverNum;
			for (Enumeration<Integer> socketList = sockList.keys(); socketList.hasMoreElements();) {
				serverNum = socketList.nextElement();
				if (serverNum != 0) { // Don't call the Policy server
					try {
						msg = new Message("PTC " + globalVersion);
						latencySleep(); // Simulate latency
						// Send
						sockList.get(serverNum).output.writeObject(msg);
						// Rec'v
						msg = (Message)sockList.get(serverNum).input.readObject();
						
						// mode 3: if all participants are using global, they
						// run auths and return YES/NO, TRUE/FALSE
						// if any are not using global, ABORT
						
						// mode 4: if any not using global, they call policy
						// server and get global, run auths, return Y/N, T/F
						// if auths fail, ABORT
						
						// Check response
						if (msg.theMessage.indexOf("NO") != -1) { // Someone responded NO
							return "ABORT PTC_RESPONSE_NO";
						}
						else if (msg.theMessage.indexOf("FALSE") != -1) { // Someone responded FALSE
							return "ABORT PTC_RESPONSE_FALSE";
						}
					}
					catch (Exception e) {
						System.err.println("Global Consistency Check Error: " + e.getMessage());
						e.printStackTrace(System.err);
					}
				}
			}
		}

		return status;
	}
	
	/**
	 * Method to run authorizations on each query performed on all participating
	 * servers, including the coordinator.
	 *
	 * @return String - COMMIT or ABORT
	 */
	public String runAuths(int version) {
		// Check local auths on coordinator
		System.out.println("Running auth. on transaction " +
						   queryLog.get(0).getTransaction() + 
						   " queries using policy version " +
						   version);
		for (int j = 0; j < queryLog.size(); j++) {
			if (!checkLocalAuth()) {
				System.out.println("Authorization of " + queryLog.get(j).getQueryType() +
								   " for transaction " + queryLog.get(j).getTransaction() +
								   ", sequence " + queryLog.get(j).getSequence() +
								   " with policy v. " + version +
								   ": FAIL");
				return "ABORT LOCAL_AUTHORIZATION_FAIL";
			}
			else {
				System.out.println("Authorization of " + queryLog.get(j).getQueryType() +
								   " for transaction " + queryLog.get(j).getTransaction() +
								   ", sequence " + queryLog.get(j).getSequence() +
								   " with policy v. " + version +
								   ": PASS");
			}
		}
		
		// Contact all other participants, have them run authorizations and return results
		if (sockList.size() > 0) {
			Message msg = null;
			int serverNum;
			
			for (Enumeration<Integer> socketList = sockList.keys(); socketList.hasMoreElements();) {
				serverNum = socketList.nextElement();
				if (serverNum != 0) { // Don't call the Policy server
					try {
						msg = new Message("RUNAUTHS " + version);
						latencySleep(); // Simulate latency
						// Send
						sockList.get(serverNum).output.writeObject(msg);
						// Rec'v
						msg = (Message)sockList.get(serverNum).input.readObject();
						// Check response, add policy version to ArrayList
						if (msg.theMessage.equals("FALSE")) {
							return "ABORT LOCAL_AUTHORIZATION_FAIL";
						}
					}
					catch (Exception e) {
						System.err.println("runAuths() error: " + e.getMessage());
						e.printStackTrace(System.err);
					}
				}
			}
		}
		
		return "COMMIT";
	}
	
	/**
	 * Method to force a policy update in order to trigger policy mismatch
	 * handling by 2PV algorithms.
	 *
	 * @param mode - the integer value of policyPush from parameters.txt file
	 */
	public void forcePolicyUpdate(int mode) {
		if (mode == 0) {
			// No push, do nothing
		}
		else if (sockList.size() > 0) { // Can only perform if more than one server
			String msgText = "POLICYPUSH";
			
			if (mode == 1) { // Push an update to a single server
				// Find the first server in the list, push an update to it
				for (int i = 1; i <= my_tm.serverList.size(); i++) {
					if (sockList.hasSocket(i)) {
						// Call policy server to update policy version on it
						System.out.println("Preparing to request policy update for server " +
										   i + ".");
						msgText += " " + i;
						break;
					}
				}
			}
			else if (mode == 2) { // Push an update to all servers
				// msgText is already set correctly
				System.out.println("Preparing to request policy update to all servers.");
			}
			else if (mode == 3) { // Update global version, but do not push
				msgText += " UPDATEONLY";
				System.out.println("Preparing to request undistributed policy update.");
			}
			// Send policy server msg, wait for ACK
			try {
				Message msg = new Message(msgText);
				// Connect to the policy server
				final Socket sock = new Socket(my_tm.serverList.get(0).getAddress(),
											   my_tm.serverList.get(0).getPort());
				// Set up I/O streams with the policy server
				final ObjectOutputStream output = new ObjectOutputStream(sock.getOutputStream());
				final ObjectInputStream input = new ObjectInputStream(sock.getInputStream());
				System.out.println("Connected to Policy Server at " +
								   my_tm.serverList.get(0).getAddress() + ":" +
								   my_tm.serverList.get(0).getPort());
				// Send
				output.writeObject(msg);
				// Rec'v ACK
				msg = (Message)input.readObject();
				if (!msg.theMessage.equals("ACK")) {
					System.err.println("*** Error with Policy Server during POLICYPUSH.");
				}
			}
			catch (Exception e) {
				System.err.println("Error: " + e.getMessage());
				e.printStackTrace(System.err);
			}
		}
	}
	
	public void databaseRead() {
		if (my_tm.threadSleep) {
			try {
				// sleep for a random period of time between 75ms and 125ms
				Thread.sleep(75 + generator.nextInt(50));
			}
			catch(Exception e) {
				System.err.println("databaseRead() Sleep Error: " + e.getMessage());
				e.printStackTrace(System.err);
			}
		}
		else {
			totalSleepTime += 75 + generator.nextInt(50);
		}
	}
	
	public void databaseWrite() {
		if (my_tm.threadSleep) {
			try {
				// sleep for a random period of time between 150ms and 225ms
				Thread.sleep(150 + generator.nextInt(75));
			}
			catch(Exception e) {
				System.err.println("databaseWrite() Sleep Error: " + e.getMessage());
				e.printStackTrace(System.err);
			}
		}
		else {
			totalSleepTime += 150 + generator.nextInt(75);
		}
	}
	
	/**
	 * Checks the local policy for authorization to data
	 *
	 * @return boolean - true if authorization check comes back OK, else false
	 */
	public boolean checkLocalAuth() {
		if (my_tm.threadSleep) {
			try {
				// sleep for a random period of time between 50ms and 150ms
				Thread.sleep(50 + generator.nextInt(100));
			}
			catch(Exception e) {
				System.err.println("checkLocalAuth() Sleep Error: " + e.getMessage());
				e.printStackTrace(System.err);
			}
		}
		else {
			totalSleepTime += 50 + generator.nextInt(100);
		}
		// Perform random success operation
		if (my_tm.localAuthSuccessRate < 1.0) {
			return coinToss(my_tm.localAuthSuccessRate);
		}
		else {
			return true;
		}
	}
	
	/**
	 * Checks the integrity of the data for the commit (2PC)
	 *
	 * @return boolean - true if integrity check comes back OK, else false
	 */
	public boolean integrityCheck() {
		// Sleep for duration of check, between 150ms and 225ms
		if (my_tm.threadSleep) {
			try {
				Thread.sleep(150 + generator.nextInt(75));
			}
			catch(Exception e) {
				System.err.println("verifyIntegrity() Sleep Error: " + e.getMessage());
				e.printStackTrace(System.err);
			}
		}
		else {
			totalSleepTime += 150 + generator.nextInt(75);
		}
		// Perform random success operation if necessary
		if (my_tm.integrityCheckSuccessRate < 1.0) {
			return coinToss(my_tm.integrityCheckSuccessRate);
		}
		else {
			return true;
		}
	}
	
	public boolean coinToss(float successRate) {
		if (generator.nextFloat() > successRate) {
			return false;
		}
		return true;
	}
	
	public void latencySleep() {
		if (my_tm.latencyMax > 0) { // There is artificial latency
			int latency;
			if (my_tm.latencyMax == my_tm.latencyMin) { // Fixed latency value
				latency = my_tm.latencyMax;
			}
			else { // Generate a random amount within range
				latency = my_tm.latencyMin + generator.nextInt(my_tm.latencyMax - my_tm.latencyMin);
			}
			if (my_tm.threadSleep) {
				try {
					// Sleep for <latency> ms
					Thread.sleep(latency);
				}
				catch(Exception e) {
					System.err.println("latencySleep() Error: " + e.getMessage());
					e.printStackTrace(System.err);
				}
			}
			else { // add int amount to log entry
				totalSleepTime += latency;
			}
		}
//
//		if (my_tm.threadSleep) {
//			if (my_tm.latencyMax > 0) { // Check if artificial latency is set
//				try {
//					// Sleep for a random period of time between min ms and max ms
//					Thread.sleep(my_tm.latencyMin + generator.nextInt(my_tm.latencyMax - my_tm.latencyMin));
//				}
//				catch(Exception e) {
//					System.err.println("latencySleep() Error: " + e.getMessage());
//					e.printStackTrace(System.err);
//				}
//			}
//		}
//		else {
//			totalSleepTime += my_tm.latencyMin + generator.nextInt(my_tm.latencyMax - my_tm.latencyMin);
//		}
	}
	
	/**
	 * A class to store sockets between servers in order to reduce the number
	 * of connections necessary during a transaction.
	 */
	public class SocketList {
		private Hashtable<Integer, SocketObject> list = new Hashtable<Integer, SocketObject>();
		
		public void addSocketObj(int serverNum, SocketObject so) {
			list.put(serverNum, so);
		}
		
		public boolean hasSocket(int serverNum) {
			if (list.containsKey(serverNum)) {
				return true;
			}
			return false;
		}
		
		public SocketObject get(int serverNum) {
			return list.get(serverNum);
		}
		
		public int size() {
			return list.size();
		}
		
		public Enumeration<Integer> keys() {
			return list.keys();
		}
		
	}
	
	class SocketObject {
		public Socket socket;
		public ObjectOutputStream output;
		public ObjectInputStream input;
		
		public SocketObject(Socket s, ObjectOutputStream oos, ObjectInputStream ois) {
			socket = s;
			output = oos;
			input = ois;
		}
	}
}

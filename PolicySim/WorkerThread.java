/**
 * File: WorkerThread.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * Based on original code "EchoThread.java" by Adam J. Lee (adamlee@cs.pitt.edu) 
 *
 * A simple server thread. This class just echoes the messages sent
 * over the socket until the socket is closed.
 */

/* TODO: is view consistency using the TM's server policy as its baseline, or
 * its worker thread's starting policy? */

import java.lang.Thread;            // We will extend Java's base Thread class
import java.net.Socket;
import java.net.ConnectException;
import java.io.ObjectInputStream;   // For reading Java objects off of the wire
import java.io.ObjectOutputStream;  // For writing Java objects to the wire
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.*;

public class WorkerThread extends Thread {
    private final Socket socket; // The socket that we'll be talking over
	private CloudServer my_tm; // The Transaction Manager that called the thread
	private SocketList sockList = new SocketList();
	private ArrayList<QueryRecord> queryLog = new ArrayList<QueryRecord>();
	private int transactionPolicyVersion = 0;
	private int totalSleepTime = 0; // used if my_tm.threadSleep == false
	private Random generator;

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
			int verificationType = 0;
			
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
				else if (msg.theMessage.indexOf("POLICYUPDATE") != -1) { // Policy update
					String msgSplit[] = msg.theMessage.split(" ");
					int update = Integer.parseInt(msgSplit[1]);
					// Check that we aren't going backwards in a race condition
					if (my_tm.getPolicy() < update) {
						my_tm.setPolicy(update);
						System.out.println("Server Policy Version updated to v." + update);
					}
					latencySleep(); // Simulate latency
					output.writeObject(new Message(msgText));
					break;
				}
				
				// Separate queries
				String queryGroup[] = msg.theMessage.split(",");
				for (int i = 0; i < queryGroup.length; i++) {
					// Handle instructions
					String query[] = queryGroup[i].split(" ");
					if (query[0].equals("B")) { // BEGIN
						System.out.println("BEGIN transaction " + query[1]);
						// Set the transaction's Policy version
						transactionPolicyVersion = my_tm.getPolicy();
						System.out.println("Transaction " + query[1] +
										   " Policy version set: " +
										   transactionPolicyVersion);
					}
					else if (query[0].equals("R")) { // READ
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
							
							// Check transaction policy against server policy
							if (checkLocalAuth() == false) {
								msgText = "ABORT LOCAL_POLICY_FAIL";
								System.out.println("ABORT LOCAL_POLICY_FAIL: " +
												   "READ for transaction " + query[1] +
												   " sequence " + query[3]);
							}
							// Check data access for error
							else if (checkDataAccess() == false) {
								msgText = "ABORT DATA_ACCESS_FAIL";
								System.out.println("ABORT DATA_ACCESS_FAIL: " +
												   "READ for transaction " + query[1] +
												   " sequence " + query[3]);
							}
							else { // OK to read
								System.out.println("READ for transaction " + query[1] +
												   " sequence " + query[3]);
								diskRead();
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
						else { // pass to server
							System.out.println("Pass READ of transaction " + query[1] +
											   " sequence " + query[3] +
											   " to server " + query[2]);
							if (passQuery(Integer.parseInt(query[2]), queryGroup[i])) {
								System.out.println("READ of transaction " + query[1] +
												   " sequence " + query[3] +
												   " to server " + query[2] +
												   " successful");
								
								// tell RobotThread to add this server to its commitStack
								// server is query[2], transaction is query[1], sequence is query[3]
								msgText = "ACS " + query[2] + " " + query[1] + " " + query[3];
							}
							else { // error in passQuery()
								System.out.println("ERROR in passQuery()");
							}
						}
					}
					else if (query[0].equals("W")) { // WRITE
						// Check server number, perform query or pass on
						if (Integer.parseInt(query[2]) == my_tm.serverNumber) { // Perform query on this server
							// Check that if a fresh Policy version is needed, it is gotten
							if (transactionPolicyVersion == 0) {
								transactionPolicyVersion = my_tm.getPolicy();
							}

							// Check transaction policy against server policy
							if (checkLocalAuth() == false) {
								msgText = "ABORT LOCAL_POLICY_FAIL";
								System.out.println("ABORT LOCAL_POLICY_FAIL: " +
												   "WRITE for transaction " + query[1] +
												   " sequence " + query[3]);
							}
							// Check data access for error
							else if (checkDataAccess() == false) {
								msgText = "ABORT DATA_ACCESS_FAIL";
								System.out.println("ABORT DATA_ACCESS_FAIL: " +
												   "WRITE for transaction " + query[1] +
												   " sequence " + query[3]);
							}
							else { // OK to write
								System.out.println("WRITE for transaction " + query[1] +
												   " sequence " + query[3]);
								diskWrite();
								// Add to query log
								if (addToQueryLog(query, transactionPolicyVersion)) {
									System.out.println("Transaction " + query[1] +
													   " sequence " + query[3] +
													   " query logged.");
								}
								else {
									System.out.println("Error logging query.");
								}
								// tell RobotThread to add this server to its commitStack
								// server is query[2], transaction is query[1], sequence is query[3]
								msgText = "ACS " + query[2] + " " + query[1] + " " + query[3];
							}
						}
						else { // pass to server
							System.out.println("Pass WRITE of transaction " + query[1] +
											   " sequence " + query[3] +
											   " to server " + query[2]);
							if (passQuery(Integer.parseInt(query[2]), queryGroup[i])) {
								System.out.println("WRITE of transaction " + query[1] +
												   " sequence " + query[3] +
												   " to server " + query[2] +
												   " successful");
								
								// tell RobotThread to add this server to its commitStack
								msgText = "ACS " + query[2] + " " + query[1] + " " + query[3];
							}
							else { // error in passQuery()
								System.out.println("ERROR in passQuery()");
							}
						}
					}
					else if (query[0].equals("POLICY")) { // POLICY
						// Return Policy version of this transaction to caller
						msgText = "VERSION " + Integer.toString(transactionPolicyVersion);
					}
					else if (query[0].equals("A")) { // Re-authorize a query
						// Query example: "A <global policy version>"
						// Retrieve policy version: Integer.parseInt(query[1])
						if (checkLocalAuth()) {
							msgText = "GLOBALPASS";
						}
						else {
							msgText = "GLOBALFAIL";
						}
					}
					else if (query[0].equals("C")) { // COMMIT
						System.out.println("COMMIT - transaction " + query[1]);
						
						if (!integrityCheck()) { // Check data integrity for commit
							msgText = "ABORT INTEGRITY_FAIL";
						}
						else {
							switch (my_tm.verificationType) {
								case 0:
								case 1:
									System.out.println("No View or Global Consistency required for transaction " + query[1]);
									break;
								case 2:
									if (viewConsistencyCheck() != 0) { // a server was not fresh
										System.out.println("*** View Consistency Policy FAIL - transaction " + query[1] + " ***");
										msgText = "ABORT VIEW_POLICY_FAIL";
									}
									else {
										System.out.println("View Consistency Policy OK - transaction " + query[1]);
									}
									break;
								case 3:
									if (!globalConsistencyCheck()) {
										System.out.println("*** Global Consistency Policy FAIL - transaction " + query[1] + " ***");
										msgText = "ABORT GLOBAL_POLICY_FAIL";
									}
									else {
										System.out.println("Global Consistency Policy OK - transaction " + query[1]);
									}
									break;
								case 4:
									if (viewConsistencyCheck() != 0) { // a server was not fresh
										System.out.println("*** View Consistency Policy FAIL - transaction " + query[1] + " ***");
										System.out.println("*** Attempting Global Consistency Check - transaction " + query[1] + " ***");
										if (!globalConsistencyCheck()) {
											System.out.println("*** Second Chance FAIL - transaction " + query[1] + " ***");
											msgText = "ABORT VIEW_AND_GLOBAL_POLICY_FAIL";
										}
										else {
											System.out.println("Global Consistency Policy OK - transaction " + query[1]);
										}
									}
									else {
										System.out.println("View Consistency Policy OK - transaction " + query[1]);
									}
									break;
							}
						}
					}
					else if (query[0].equals("S")) { // Sleep for debugging
						Thread.sleep(Integer.parseInt(query[1]));
					}
					else if (query[0].toUpperCase().equals("EXIT")) { // end of transaction
						// send exit flag to RobotThread
						msgText = "FIN";
					}
				}
				latencySleep(); // Simulate latency
				// ACK completion of this query group
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
	 * @return boolean - true if query was successful, else false
	 */
	public boolean passQuery(int otherServer, String query) {
		String server = my_tm.serverList.get(otherServer).getAddress();
		int port = my_tm.serverList.get(otherServer).getPort();
		
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
			}

			// send query
			Message msg = null;
			msg = new Message(query);
			latencySleep(); // Simulate latency
			sockList.get(otherServer).output.writeObject(msg);
			msg = (Message)sockList.get(otherServer).input.readObject();
			System.out.println("Server " + otherServer +
							   " says: " + msg.theMessage +
							   " for passed query " + query);
			// Get policy version and log the query
			msg = new Message("POLICY");
			latencySleep(); // Simulate latency
			sockList.get(otherServer).output.writeObject(msg);
			msg = (Message)sockList.get(otherServer).input.readObject();
			String msgSplit[] = msg.theMessage.split(" ");
			if (addToQueryLog(query.split(" "), Integer.parseInt(msgSplit[1]))) {
				return true;
			}
		}
		catch (ConnectException ce) {
			System.err.println(ce.getMessage() +
							   ": Check server address and port number.");
			ce.printStackTrace(System.err);
		}
		catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
		return false;
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
	 * Checks the integrity of the data for the commit
	 *
	 * @return boolean - true if integrity check comes back OK, else false
	 */
	public boolean integrityCheck() {
		try {
			// sleep for a random period of time between 150ms and 225ms
			Thread.sleep(150 + generator.nextInt(75));
		}
		catch(Exception e) {
			System.err.println("verifyIntegrity() Sleep Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
		// perform random success operation
		return true;
	}

	public void diskRead() {
		try {
			// sleep for a random period of time between 75ms and 125ms
			Thread.sleep(75 + generator.nextInt(50));
		}
		catch(Exception e) {
			System.err.println("diskRead() Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
	}
	
	public void diskWrite() {
		try {
			// sleep for a random period of time between 150ms and 225ms
			Thread.sleep(150 + generator.nextInt(75));
		}
		catch(Exception e) {
			System.err.println("diskWrite() Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
	}
	
	/**
	 * Checks if accessing requested data was successful
	 *
	 * @return boolean - true if access was successful, else false
	 */
	public boolean checkDataAccess() {
		/* STUB */
		// perform random success operation
		return true;
	}
	
	/**
	 * Checks the local policy for authorization to data
	 *
	 * @return boolean - true if authorization check comes back OK, else false
	 */
	public boolean checkLocalAuth() {
		if (my_tm.verificationType > 0) { // requires local authorization
			try {
				// sleep for a random period of time between 50ms and 150ms
				Thread.sleep(50 + generator.nextInt(100));
			}
			catch(Exception e) {
				System.err.println("checkLocalAuth() Sleep Error: " + e.getMessage());
				e.printStackTrace(System.err);
			}
			// Perform random success operation
			return coinToss(my_tm.localAuthSuccessRate);
		}
		else { // just 2PC, no check needed
			return true;
		}
	}
	
	/**
	 * Checks all involved servers for Policy version freshness
	 *
	 * @return int - 0 if all servers are fresh, 1+ if not
	 */
	public int viewConsistencyCheck() {
		int masterPolicyVersion = my_tm.getPolicy(); // store current policy on server
		int stale = 0;
		Message msg = null;
		Message resp = null;
		
		if (sockList.size() > 0) {
			int serverNum;
			for (Enumeration<Integer> socketList = sockList.keys(); socketList.hasMoreElements();) {
				serverNum = socketList.nextElement();
				if (serverNum != 0) { // Don't call the Policy server
					try {
						msg = new Message("POLICY");
						latencySleep(); // Simulate latency
						sockList.get(serverNum).output.writeObject(msg);
						resp = (Message)sockList.get(serverNum).input.readObject();
						// Compare Policy versions
						String msgSplit[] = resp.theMessage.split(" ");
						if (msgSplit[0].equals("VERSION") && Integer.parseInt(msgSplit[1]) < masterPolicyVersion) {
							stale++;
						}
					}
					catch (Exception e) {
						System.err.println("Policy Check Error: " + e.getMessage());
						e.printStackTrace(System.err);
					}
				}
			}
		}
		return stale;
	}

	public boolean globalConsistencyCheck() {
		int masterPolicyVersion = my_tm.callPolicyServer(); // store freshest policy off policy server
		
		for (int i = 0; i < queryLog.size(); i++) {
			if (queryLog.get(i).getPolicy() != masterPolicyVersion) {
				// Re-check authorization with new policy version				
				if (queryLog.get(i).getServer() == my_tm.serverNumber) { // local check
					if (checkLocalAuth() == false) {
						System.out.println("Global Consistency Check FAIL: " + queryLog.get(i).toString() +
										   " version: " + queryLog.get(i).getPolicy() +
										   "\tGlobal version: " + masterPolicyVersion);
						return false;
					}
				}
				else { // other server? passQuery(<server number>, "A <masterPolicyVersion>")
					int otherServer = queryLog.get(i).getServer();
					String server = my_tm.serverList.get(otherServer).getAddress();
					int port = my_tm.serverList.get(otherServer).getPort();
					
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
						}
						
						// Send query for global auth
						Message msg = null;
						msg = new Message("A " + masterPolicyVersion);
						latencySleep(); // Simulate latency
						sockList.get(otherServer).output.writeObject(msg);
						msg = (Message)sockList.get(otherServer).input.readObject();
						System.out.println("Server " + otherServer +
										   " says: " + msg.theMessage +
										   " for passed query A " + masterPolicyVersion);
						if (msg.theMessage.equals("GLOBALFAIL")) {
							System.out.println("Global Consistency Check FAIL: " + queryLog.get(i).toString() +
											   " version: " + queryLog.get(i).getPolicy() +
											   "\tGlobal version: " + masterPolicyVersion);
							return false;
						}
					}
					catch (ConnectException ce) {
						System.err.println(ce.getMessage() +
										   ": Check server address and port number.");
						ce.printStackTrace(System.err);
					}
					catch (Exception e) {
						System.err.println("Error: " + e.getMessage());
						e.printStackTrace(System.err);
					}
				}
			}
		}
		return true;
	}

	public boolean coinToss(float successRate) {
		if (generator.nextFloat() > successRate) {
			return false;
		}
		return true;
	}
	
	public void latencySleep() {
		int latency = my_tm.latencyMin + generator.nextInt(my_tm.latencyMax - my_tm.latencyMin);
		if (my_tm.threadSleep) {
			try {
				// Sleep for a random period of time between min ms and max ms
				Thread.sleep(latency);
			}
			catch(Exception e) {
				System.err.println("latencySleep() Error: " + e.getMessage());
				e.printStackTrace(System.err);
			}
		}
		else {
			totalSleepTime += latency;
		}
	}
	
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

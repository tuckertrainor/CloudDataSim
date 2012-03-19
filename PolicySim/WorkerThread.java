/**
 * File: WorkerThread.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * Based on original code "EchoThread.java" by Adam J. Lee (adamlee@cs.pitt.edu) 
 *
 * A simple server thread. This class just echoes the messages sent
 * over the socket until the socket is closed.
 */

/* TODO: randomize read/write sleep times, set up deferred policy check at end
 */

import java.lang.Thread;            // We will extend Java's base Thread class
import java.net.Socket;
import java.net.ConnectException;
import java.io.ObjectInputStream;   // For reading Java objects off of the wire
import java.io.ObjectOutputStream;  // For writing Java objects to the wire
import java.util.ArrayList;
import java.util.Date;
import java.util.*;

public class WorkerThread extends Thread {
    private final Socket socket; // The socket that we'll be talking over
    private final int serverNumber; // The number of this server
	private final ArrayList<ServerID> serverList;
	private final int readSleep = 5; // number of milliseconds for a READ
	private final int writeSleep = 5; // number of milliseconds for a WRITE
	private SocketList sockList = new SocketList();
	private int transactionPolicyVersion;

	/**
	 * Constructor that sets up the socket we'll chat over
	 *
	 * @param _socket - The socket passed in from the server
	 * @param _serverNumber - The number assigned to the server that created thread
	 * @param _serverList - the list of server IDs for connection
	 */
	public WorkerThread(Socket _socket, int _serverNumber, ArrayList<ServerID> _serverList) {
		socket = _socket;
		serverNumber = _serverNumber;
		serverList = _serverList;
	}

	/**
	 * run() is basically the main method of a thread. This thread
	 * simply reads Message objects off of the socket.
	 */
	public void run() {
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
				
				// Separate queries
				String queryGroup[] = msg.theMessage.split(",");
				for (int i = 0; i < queryGroup.length; i++) {
					// Handle instructions
					String query[] = queryGroup[i].split(" ");
					if (query[0].equals("B")) { // BEGIN
						System.out.println("BEGIN transaction " + query[1]);
						// Get the transaction's Policy version
						transactionPolicyVersion = getCurrentPolicy();
						System.out.println("Policy version set: " + transactionPolicyVersion);
					}
					else if (query[0].equals("R")) { // READ
						// Check server number, perform query or pass on
						if (Integer.parseInt(query[2]) == serverNumber) { // Perform query on this server
							if (accessData() == false) {
								// message an error, abort transaction
							}
							else {
								// add time to counter or sleep
								System.out.println("READ for transaction " + query[1]);
								Thread.sleep(readSleep);
							}
						}
						else { // pass to server
							System.out.println("Pass READ of transaction " + query[1] +
											   " to server " + query[2]);
							if (passQuery(Integer.parseInt(query[2]), queryGroup[i])) {
								System.out.println("READ of transaction " + query[1] +
												   " to server " + query[2] +
												   " successful");
							}
							else { // error in passQuery()
								System.out.println("ERROR in passQuery()");
							}
						}
					}
					else if (query[0].equals("W")) { // WRITE
						// Check server number, perform query or pass on
						if (Integer.parseInt(query[2]) == serverNumber) { // Perform query on this server
							if (accessData() == false) {
								// message an error, abort transaction
							}
							else {
								System.out.println("WRITE for transaction " + query[1]);
								// add time to counter or sleep
								
								// tell RobotThread to add this server to its commitStack
								msgText = "ACS " + query[2];

								Thread.sleep(writeSleep);
							}
						}
						else { // pass to server
							System.out.println("Pass WRITE of transaction " + query[1] +
											   " to server " + query[2]);
							if (passQuery(Integer.parseInt(query[2]), queryGroup[i])) {
								System.out.println("WRITE of transaction " + query[1] +
												   " to server " + query[2] +
												   " successful");
								
								// tell RobotThread to add this server to its commitStack
								msgText = "ACS " + query[2];
							}
							else { // error in passQuery()
								System.out.println("ERROR in passQuery()");
							}
						}
					}
					else if (query[0].equals("C")) { // COMMIT
						// will need to keep list of all servers accessed, use it
						// to finalize commit across servers
						System.out.println("COMMIT STUB - transaction " + query[1]);
						
						// call verifyIntegrity() here?
					}
					else if (query[0].toUpperCase().equals("EXIT")) { // end of transaction
						// send exit flag to RobotThread
						msgText = "FIN";
					}
				}
				// ACK completion of this query group
				output.writeObject(new Message(msgText));
			}
			// Close any SocketGroup connection
			if (sockList.size() > 0) {
				int serverNum;
				for (Enumeration<Integer> socketList = sockList.keys(); socketList.hasMoreElements();) {
					msg = new Message("DONE");
					serverNum = socketList.nextElement();
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
		String server = serverList.get(otherServer).getAddress();
		int port = serverList.get(otherServer).getPort();
		
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

			Message msg = null, resp = null;
			
			// send query
			msg = new Message(query);
			sockList.get(otherServer).output.writeObject(msg);
			resp = (Message)sockList.get(otherServer).input.readObject();
			System.out.println("Server " + otherServer +
							   " says: " + resp.theMessage);			
			return true;
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
	
	/**
	 * Checks the integrity of the data for the commit
	 *
	 * @return boolean - true if integrity check comes back OK, else false
	 */
	public boolean verifyIntegrity() {
		System.out.println("verifyIntegrity() stub");
		// perform random success operation
		return true;
	}

	/**
	 * Checks if accessing requested data was successful
	 *
	 * @return boolean - true if access was successful, else false
	 */
	public boolean accessData() {
		System.out.println("accessData() stub");
		// perform random success operation
		return true;
	}
	
	/**
	 * Checks the local policy for authorization to data
	 *
	 * @return boolean - true if authorization check comes back OK, else false
	 */
	public boolean getLocalAuth() {
		System.out.println("getLocalAuth() stub");
		// perform random success operation
		return true;
	}
	
	/**
	 * Checks the global policy for authorization to data
	 *
	 * @return boolean - true if authorization check comes back OK, else false
	 */
	public boolean getPolicyAuth() {
		System.out.println("getPolicyAuth() stub");
		// perform random success operation
		return true;
	}
	
	/**
	 * Calls the Policy Server for the freshest policy
	 *
	 * @return int - the current Policy version number
	 */
	public int getCurrentPolicy() {
		int policyServerID = 0;
		String server = serverList.get(policyServerID).getAddress();
		int port = serverList.get(policyServerID).getPort();
		
		try {
			// Check SocketList for an existing socket, else create and add new
			if (!sockList.hasSocket(policyServerID)) {
				// Create new socket, add it to SocketGroup
				System.out.println("Connecting to " + server +
								   " on port " + port);
				Socket sock = new Socket(server, port);
				sockList.addSocketObj(policyServerID, new SocketObject(sock,
																	   new ObjectOutputStream(sock.getOutputStream()),	
																	   new ObjectInputStream(sock.getInputStream())));
			}
			
			Message msg = null, resp = null;
			
			// Get the current Policy version
			msg = new Message("GET");
			sockList.get(policyServerID).output.writeObject(msg);
			resp = (Message)sockList.get(policyServerID).input.readObject();
			System.out.println("Policy Server says: " + resp.theMessage);
			String msgSplit[] = resp.theMessage.split(" ");
			if (msgSplit[0].equals("VERSION")) {
				return Integer.parseInt(msgSplit[1]); // return the version
			}
		}
		catch (ConnectException ce) {
			System.err.println(ce.getMessage() +
							   ": Check Policy Server address and port number.");
			ce.printStackTrace(System.err);
		}
		catch (Exception e) {
			System.err.println("Policy Server Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}		
		return 0; // ABORT or FAIL message rec'd
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

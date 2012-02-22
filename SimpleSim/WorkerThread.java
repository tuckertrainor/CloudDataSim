/**
 * File: WorkerThread.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * Based on original code "EchoThread.java" by Adam J. Lee (adamlee@cs.pitt.edu) 
 *
 * A simple server thread. This class just echoes the messages sent
 * over the socket until the socket is closed.
 */

import java.lang.Thread;            // We will extend Java's base Thread class
import java.net.Socket;
import java.io.ObjectInputStream;   // For reading Java objects off of the wire
import java.io.ObjectOutputStream;  // For writing Java objects to the wire

public class WorkerThread extends Thread {
    private final Socket socket; // The socket that we'll be talking over
    private final int serverNumber; // The number of this server

	/**
	 * Constructor that sets up the socket we'll chat over
	 *
	 * @param _socket - The socket passed in from the server
	 * @param _socket - The number assigned to the server that created thread
	 */
	public WorkerThread(Socket _socket, int _serverNumber) {
		socket = _socket;
		serverNumber = _serverNumber;
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

			// set up I/O streams with the client
			final ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
			final ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());

			// Loop to read messages
			Message msg = null;
			int count = 0;
			do {
				// read and print message
				msg = (Message)input.readObject();
				System.out.println("[" + socket.getInetAddress() +
								   ":" + socket.getPort() + "] " + msg.theMessage);

				// Write an ACK back to the sender
				count++;
				String instr[] = msg.theMessage.split(" ");
				if (instr.length != 1) {
					output.writeObject(new Message("Received message #" + count +
												   " of transaction " + instr[1]));
				}
				else {
					output.writeObject(new Message("Received message #" + count));
				}
				
				// Handle instructions
				if (instr[0].equals("R")) { // READ
					// Check server number, perform query or pass on
					if (Integer.parseInt(instr[2]) == serverNumber) { // Perform query on this server
						if (accessData() == false) {
							// message an error, abort transaction
						}
						else {
							// add time to counter or sleep
						}
					}
					else { // pass to server
						System.out.println("Pass READ of transaction " + instr[1] +
										   " to server " + instr[2]);
					}
				}
				else if (instr[0].equals("W")) { // WRITE
					// Check server number, perform query or pass on
					if (Integer.parseInt(instr[2]) == serverNumber) { // Perform query on this server
						if (accessData() == false) {
							// message an error, abort transaction
						}
						else {
							// add time to counter or sleep
						}
					}
					else { // pass to server
						System.out.println("Pass WRITE of transaction " + instr[1] +
										   " to server " + instr[2]);
					}
				}
				else if (instr[0].equals("C")) { // COMMIT
					// will need to keep list of all servers accessed, use it
					// to finalize commit across servers
					System.out.println("COMMIT STUB");
				}
			} while(!msg.theMessage.toUpperCase().equals("EXIT"));

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
	 * Checks the integrity of the data for the commit
	 *
	 * @return boolean - true if integrity check comes back OK, else false
	 */
	public static boolean verifyIntegrity() {
		System.out.println("verifyIntegrity() stub");
		// perform random success operation
		return true;
	}
	/**
	 * Checks if accessing requested data was successful
	 *
	 * @return boolean - true if access was successful, else false
	 */
	public static boolean accessData() {
		System.out.println("accessData() stub");
		// perform random success operation
		return true;
	}
	
	/**
	 * Checks the local policy for authorization to data
	 *
	 * @return boolean - true if authorization check comes back OK, else false
	 */
	public static boolean getLocalAuth() {
		System.out.println("getLocalAuth() stub");
		// perform random success operation
		return true;
	}
	
	/**
	 * Checks the global policy for authorization to data
	 *
	 * @return boolean - true if authorization check comes back OK, else false
	 */
	public static boolean getPolicyAuth() {
		System.out.println("getPolicyAuth() stub");
		// perform random success operation
		return true;
	}
	
	/**
	 * Calls the Certificate Authority for the freshest policy
	 *
	 * @return boolean - true if freshest policy is received/good, else false
	 */
	public static boolean checkPolicyWithCA() {
		System.out.println("checkPolicyWithCA() stub");
		// call the CA server, get results of call back
		return true;
	}
}
/**
 * File: PolicyRequestThread.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A thread to handle Policy version requests from CloudServers.
 */

import java.lang.Thread;
import java.net.Socket;
import java.net.ConnectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class PolicyRequestThread extends Thread {
    private final Socket socket; // The socket that we'll be talking over
	
	/**
	 * Constructor that sets up the thread
	 *
	 * @param _socket
	 * @param _my_ps
	 */
	public PolicyRequestThread(Socket _socket) {
		socket = _socket;
	}
	
	public void run() {
		try {
			// Print incoming message
			System.out.println("** Policy Version request from " + socket.getInetAddress() +
							   ":" + socket.getPort() + " **");
			
			// Set up I/O streams with the calling thread
			final ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
			final ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
			
			Message msg = null;

			// Read and print message
			msg = (Message)input.readObject();
			System.out.println("[" + socket.getInetAddress() +
							   ":" + socket.getPort() + "] " + msg.theMessage);
			
			if (msg.theMessage.equals("POLICYREQUEST")) {
				output.writeObject(new Message("" + PolicyVersion.getCurrent()));
			}
			else {
				output.writeObject(new Message("FAIL"));
			}
			
			// Close the connection
			socket.close();
		}
		catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
	}
}
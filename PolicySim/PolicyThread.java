/**
 * File: PolicyThread.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A thread to handle Policy updates to active CloudServers.
 */

import java.lang.Thread;
import java.net.Socket;
import java.net.ConnectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

public class PolicyThread extends Thread {
    private final int version;
	private final String address;
	private final int port;
	private final int latency;
	
	/**
	 * Constructor that sets up the socket we'll chat over
	 *
	 * @param _version
	 * @param _address
	 * @param _port
	 * @param _latency
	 */
	public PolicyThread(int _version, String _address, int _port, int _latency) {
		version = _version;
		address = _address;
		port = _port;
		latency = _latency;
	}
	
	/**
	 * run() is basically the main method of a thread.
	 */
	public void run() {
		try {
			final Socket socket = new Socket(address, port);
			System.out.println("** Pushing Policy update to " + socket.getInetAddress() +
							   ":" + socket.getPort() + " **");
			
			// Set up I/O streams with the server
			final ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
			final ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
			
			Message msg = null;
			Message response = null;
			
			if (latency > 0) {
				// Sleep to simulate latency
				try {
					Thread.sleep(latency);
				}
				catch(Exception e) {
					System.err.println("latencySleep() Error: " + e.getMessage());
					e.printStackTrace(System.err);
				}
			}
			msg = new Message("POLICYUPDATE " + version);
			output.writeObject(msg);
			response = (Message)input.readObject();
			if (!response.theMessage.equals("ACK")) {
				System.out.println("Error: Incorrect ACK from " + socket.getInetAddress() +
								   ":" + socket.getPort());
			}

			socket.close();
		}
		catch(ConnectException ce) {
			System.out.println("** Connect Exception for " + address +
							   ":" + port + " - could not push update **");
		}
		catch(Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
	}
}

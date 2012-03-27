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
	private final int minSleep;
	private final int maxSleep;
	
	/**
	 * Constructor that sets up the socket we'll chat over
	 *
	 * @param _version
	 * @param _address
	 * @param _port
	 * @param _minSleep
	 * @param _maxSleep
	 */
	public PolicyThread(int _version, String _address, int _port, int _minSleep, int _maxSleep) {
		version = _version;
		address = _address;
		port = _port;
		minSleep = _minSleep;
		maxSleep = _maxSleep;
	}
	
	/**
	 * run() is basically the main method of a thread.
	 */
	public void run() {
		try {
			// Print incoming message
			Socket socket = new Socket(address, port);
			System.out.println("** Pushing policy update to " + socket.getInetAddress() +
							   ":" + socket.getPort() + " **");
			
			// Set up I/O streams with the calling thread
			final ObjectInputStream input = new ObjectInputStream(socket.getInputStream());
			final ObjectOutputStream output = new ObjectOutputStream(socket.getOutputStream());
			
			Message msg = null;
			Message response = null;
			
			if (minSleep + maxSleep > 0) { // sleep before push
				Random generator = new Random(new Date().getTime());
				Thread.sleep(minSleep + generator.nextInt(maxSleep - minSleep));
			}

			output.writeObject(new Message("POLICYUPDATE " + version));
			response = (Message)input.readObject();
			if (!response.theMessage.equals("ACK")) {
				System.out.println("Error: No ACK from " + socket.getInetAddress() +
								   ":" + socket.getPort());
			}

			output.writeObject(new Message("DONE"));
			socket.close();

		}
		catch(Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
	}
}

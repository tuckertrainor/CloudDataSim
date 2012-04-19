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
import java.util.Date;
import java.util.Random;

public class PolicyRequestThread extends Thread {
    private final Socket socket; // The socket that we'll be talking over
	private final int latencyMin;
	private final int latencyMax;
	
	/**
	 * Constructor that sets up the thread
	 *
	 * @param _socket
	 * @param _latency
	 */
	public PolicyRequestThread(Socket _socket, int _lMin, int _lMax) {
		socket = _socket;
		latencyMin = _lMin;
		latencyMax = _lMax;
	}
	
	public void run() {
		try {
			// Create and seed random number generator
			Random generator = new Random(new Date().getTime());
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
			
			// Sleep to simulate latency of response
			try {
				Thread.sleep(latencyMin + generator.nextInt(latencyMax - latencyMin));
			}
			catch(Exception e) {
				System.err.println("latencySleep() Error: " + e.getMessage());
				e.printStackTrace(System.err);
			}
			
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
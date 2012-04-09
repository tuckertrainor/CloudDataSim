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
	private PolicyServer my_ps; // The PolicyServer that called the thread
	
	/**
	 * Constructor that sets up the thread
	 *
	 * @param _socket
	 * @param _my_ps
	 */
	public PolicyUpdater(Socket _socket, PolicyServer _my_ps) {
		socket = _socket;
		my_ps = _my_ps;
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
				else if (msg.theMessage.indexOf("CURRENTPOLICY") != -1) {
					msgText = "" + PolicyVersion.getCurrent();
					output.writeObject(new Message(msgText));
				}
			}
		}
		catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
	}
}
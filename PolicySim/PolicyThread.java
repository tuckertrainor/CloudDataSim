/**
 * File: PolicyThread.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A simple server thread to handle Policy authorization calls.
 */

import java.lang.Thread;
import java.net.Socket;
import java.net.ConnectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.*;

public class PolicyThread extends Thread {
    private final Socket socket; // The socket that we'll be talking over
	private final int authSleep = 5; // number of milliseconds for an AUTH
	
	/**
	 * Constructor that sets up the socket we'll chat over
	 *
	 * @param _socket - The socket passed in from the server
	 */
	public PolicyThread(Socket _socket) {
		socket = _socket;
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
				String msgText = "CURRENT";
				// Read and print message
				msg = (Message)input.readObject();
				System.out.println("[" + socket.getInetAddress() +
								   ":" + socket.getPort() + "] " + msg.theMessage);
				String msgSplit[] = msg.theMessage.split(" ");
				
				if (msgSplit[0].equals("DONE")) {
					break;
				}
				else if (msgSplit[0].equals("GET")) {
					msgText = "VERSION " + PolicyVersion.getCurrent();
				}
				else if (msgSplit[0].equals("AUTH")) {
					Thread.sleep(authSleep);
					int userPolicy = Integer.parseInt(msgSplit[1]);
					if (userPolicy != PolicyVersion.getCurrent()) {
						// deferred, punctual, inc. punctual:
						msgText = "STALE";
						
						// continuous:
						// allow update of policies on all servers
					}
					// if (random fail) {
					// msgText = "ABORT";
					// }
					// will require calling thread to send a DONE msg once
					// ABORT is rec'd
				}
				else { // unknown request
					msgText = "FAIL";
				}

				// Send results of auth back to calling server
				output.writeObject(new Message(msgText));
			}
			// Close any SocketGroup connection
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
}

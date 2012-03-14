/**
 * File: CertAuthThread.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A simple server thread to handle CA calls.
 */

import java.lang.Thread;
import java.net.Socket;
import java.net.ConnectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.*;

public class CertAuthThread extends Thread {
    private final Socket socket; // The socket that we'll be talking over
	private final int authSleep = 5; // number of milliseconds for an AUTH
	
	/**
	 * Constructor that sets up the socket we'll chat over
	 *
	 * @param _socket - The socket passed in from the server
	 */
	public CertAuthThread(Socket _socket) {
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
				String msgText = "ACK";
				// Read and print message
				msg = (Message)input.readObject();
				System.out.println("[" + socket.getInetAddress() +
								   ":" + socket.getPort() + "] " + msg.theMessage);
				
				if (msg.theMessage.equals("DONE")) {
					break;
				}
				
				if (msg.theMessage.equals("AUTH")) {
					// do auth, randomize failure
					Thread.sleep(authSleep);
					
					// if (random fail) {
					// msgText = "ABORT";
					// }
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

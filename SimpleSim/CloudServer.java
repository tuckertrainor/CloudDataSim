/**
 * File: CloudServer.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * Based on original code "EchoServer.java" by Adam J. Lee (adamlee@cs.pitt.edu) 
 *
 * A simple server class. Accepts client connections and forks
 * WorkerThreads to handle the bulk of the work.
 */

import java.net.ServerSocket;  // The server uses this to bind to a port
import java.net.Socket;        // Incoming connections are represented as sockets

public class CloudServer {
    /**
     * Main routine. Just a dumb loop that keeps accepting new
     * client connections.
     */
    public static void main(String[] args) {
		int serverPort = 8765; // default port number
		// Get new port number from command line, otherwise use default
		if (args.length == 1) {
			try {
				serverPort = Integer.parseInt(args[0]);
			}
			catch (Exception e) {
				System.err.println("Error parsing argument. Please use valid integer.");
				System.err.println("Usage: java CloudServer <Port Number>\n");
				System.exit(-1);
			}
	    }

		try {
			// This is basically just listens for new client connections
			final ServerSocket serverSock = new ServerSocket(serverPort);
			
			// A simple infinite loop to accept connections
			Socket sock = null;
			WorkerThread thread = null;
			while(true) {
				sock = serverSock.accept();			// Accept an incoming connection
				thread = new WorkerThread(sock);	// Create a thread to handle this connection
				thread.start();						// Fork the thread
			}										// Loop to work on new connections while this
													// the accept()ed connection is handled
		}
		catch(Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
    }
}
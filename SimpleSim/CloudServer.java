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
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

public class CloudServer {
    /**
     * Main routine. Just a dumb loop that keeps accepting new
     * client connections.
     */
    public static void main(String[] args) {
		// call loadConfig() here
		
		int serverNumber = 1; // should be set by loadConfig, passed to thread
		int serverPort = 8765; // default port number
		// Get new port number from command line, otherwise use default
		if (args.length == 2) {
			try {
				serverNumber = Integer.parseInt(args[0]);
				serverPort = Integer.parseInt(args[1]);
			}
			catch (Exception e) {
				System.err.println("Error parsing argument. Please use valid integers.");
				System.err.println("Usage: java CloudServer <Server Number> <Port Number>\n");
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
				sock = serverSock.accept(); // Accept an incoming connection
				thread = new WorkerThread(sock, serverNumber); // Create a thread to handle this connection
				thread.start(); // Fork the thread
			}					// Loop to work on new connections while this
								// the accept()ed connection is handled
		}
		catch(Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
    }
	
	/**
	 * Loads the configuration file for this server
	 *
	 * @return boolean - true if file loaded successfully, else false
	 */
	public static boolean loadConfig() {
		// load the local configuration file
		BufferedReader inputBuf = null;
		String line = null;
		// use a try/catch block to open the input file with a FileReader
		try {
			inputBuf = new BufferedReader(new FileReader("serverConfig.txt"));
		}
		catch (FileNotFoundException fnfe) {
			// if the file is not found, exit the program
			System.out.println("File \"serverConfig.txt\" not found.");
			fnfe.printStackTrace();
			return false;
		}
		// read a line from the file using a try/catch block
		try {
			line = inputBuf.readLine();
			System.out.println(line);
		}
		catch (IOException ioe) {
			System.out.println("IOException during readLine().");
			ioe.printStackTrace();
			return false;
		}
		
		// close BufferedReader using a try/catch block
		try {
			inputBuf.close();
			
		}
		catch (IOException ioe) {
			// if exception caught, exit the program
			System.out.println("Error closing reader.");
			ioe.printStackTrace();
			return false;
		}
		
		return true;
	}
}
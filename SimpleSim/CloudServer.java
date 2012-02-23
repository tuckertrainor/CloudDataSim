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
import java.util.ArrayList;

public class CloudServer {
	static ArrayList<ServerID> serverList;
	
    /**
     * Main routine. Just a dumb loop that keeps accepting new
     * client connections.
     */
    public static void main(String[] args) {
		// Load server information from configuration file
		if (!loadConfig()) {
			System.err.println("Error loading configuration file. Exiting.");
			System.exit(-1);
		}
		else {
			System.out.println("booya");
		}
		
		int serverNumber = 0;
		// Get new server number from command line
		if (args.length == 1) {
			try {
				serverNumber = Integer.parseInt(args[0]);
			}
			catch (Exception e) {
				System.err.println("Error parsing argument. Please use valid integers.");
				System.err.println("Usage: java CloudServer <Server Number>\n");
				System.exit(-1);
			}
	    }
		
		// test loadConfig
		System.out.println(serverNumber + ": " +
						   serverList.get(serverNumber).getNumber() + " " +
						   serverList.get(serverNumber).getAddress() + " " +
						   serverList.get(serverNumber).getPort());

		try {
			// This is basically just listens for new client connections
			final ServerSocket serverSock = new ServerSocket(serverList.get(serverNumber).getPort());
			
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
		}
		catch (IOException ioe) {
			System.out.println("IOException during readLine().");
			ioe.printStackTrace();
			return false;
		}

		while (line != null) {
			System.out.println("hi");

			if (line.charAt(0) != '#') { // not a comment
				try {
					String temp = line.split("\n")[0];
					String triplet[] = temp.split(" ");
					System.out.println(triplet[0] + " " + triplet[1] + " " + triplet[2]);
					System.out.println(Integer.parseInt(triplet[0]));
					System.out.println(triplet[1]);
					System.out.println(Integer.parseInt(triplet[2]));
					ServerID newServer = new ServerID(Integer.parseInt(triplet[0]),
											 triplet[1],
											 Integer.parseInt(triplet[2]));
					serverList.add(newServer);
				}
				catch (Exception e) {
					System.out.println("Error while parsing \"serverConfig.txt\".");
					e.printStackTrace();
					return false;
				}
				// read next line
				try {
					line = inputBuf.readLine();
				}
				catch (IOException ioe) {
					System.out.println("IOException during readLine().");
					ioe.printStackTrace();
					return false;
				}
			}
			else { // it's a comment, skip to next line
				try {
					line = inputBuf.readLine();
				}
				catch (IOException ioe) {
					System.out.println("IOException during readLine().");
					ioe.printStackTrace();
					return false;
				}
			}
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
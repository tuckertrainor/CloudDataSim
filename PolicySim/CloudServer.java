/**
 * File: CloudServer.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * Based on original code "EchoServer.java" by Adam J. Lee (adamlee@cs.pitt.edu) 
 *
 * A simple server class. Accepts client connections and forks
 * WorkerThreads to handle the bulk of the work.
 */

import java.net.ServerSocket;
import java.net.Socket;
import java.io.*;
import java.util.ArrayList;

public class CloudServer {
	public static ArrayList<ServerID> serverList;
	public int serverNumber;
	private int serverPolicyVersion;
	private static boolean verbose = false;
	
	public CloudServer(int _serverNumber) {
		serverNumber = _serverNumber;
	}

    /**
     * Main routine. Loads the parameters and starts the server.
     */
    public static void main(String[] args) {
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
		else if (args.length == 2) {
			try {
				serverNumber = Integer.parseInt(args[0]);
				if (args[1].equalsIgnoreCase("V")) {
					verbose = true;
				}
			}
			catch (Exception e) {
				System.err.println("Error parsing argument. Please use valid integers.");
				System.err.println("Usage: java CloudServer <Server Number>\n");
				System.exit(-1);
			}
	    }
		
		// Load server information from configuration file
		serverList = loadConfig("serverConfig.txt");
		if (serverList == null) {
			System.err.println("Error loading configuration file. Exiting.");
			System.exit(-1);
		}
		else {
			System.out.println("Configuration file loaded. Server " +
							   serverNumber + " is ready.");
		}
		
		CloudServer server = new CloudServer(serverNumber);
		// Set the currect policy on this server from the Policy Server
		server.setPolicy(server.callPolicyServer());
		if (server.serverPolicyVersion == 0) {
			System.out.println("Error retrieving Policy Version from Policy Server");
			System.exit(-1);
		}
		// Start listening for client connections
		server.start();
	}
	
    /**
     * Server start routine. Just a dumb loop that keeps accepting new
     * client connections.
     */
	public void start() {
		try {
			// This is basically just listens for new client connections
			final ServerSocket serverSock = new ServerSocket(serverList.get(serverNumber).getPort());
			
			// A simple infinite loop to accept connections
			Socket sock = null;
			WorkerThread thread = null;
			while(true) {
				// Accept an incoming connection
				sock = serverSock.accept();
				// Create a thread to handle this connection
				thread = new WorkerThread(sock, this, verbose);
				thread.start(); // Fork the thread
			}					// Loop to work on new connections while this
								// the accept()ed connection is handled
		}
		catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
    }
	
	public int getPolicy() {
		return serverPolicyVersion;
	}
	
	public void setPolicy(int update) {
		serverPolicyVersion = update;
		if (verbose) {
			System.out.println("Server Policy Version updated to v. " + update);
		}
	}
	
	public int callPolicyServer() {
		try {
			// Connect to the Policy Server
			final Socket policySocket = new Socket(serverList.get(0).getAddress(),
												   serverList.get(0).getPort());
			if (verbose) {
				System.out.println("CloudServer " + serverNumber + " calling Policy Server");
			}
			// Set up I/O streams with the server
			final ObjectOutputStream output = new ObjectOutputStream(policySocket.getOutputStream());
			final ObjectInputStream input = new ObjectInputStream(policySocket.getInputStream());
			// Send message
			Message msg = new Message("POLICYREQUEST");
			output.writeObject(msg);
			// Receive response
			msg = (Message)input.readObject();
			if (msg.theMessage.equals("FAIL")) {
				System.out.println("*** CloudServer Policy Request FAIL ***");
			}
			else {
				policySocket.close();
				return Integer.parseInt(msg.theMessage);
			}
		}
		catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
		return 0; // FAIL
	}
	
	/**
	 * Loads the configuration file for this server
	 *
	 * @return boolean - true if file loaded successfully, else false
	 */
	public static ArrayList<ServerID> loadConfig(String filename) {
		BufferedReader inputBuf = null;
		String line = null;
		ArrayList<ServerID> configList = new ArrayList<ServerID>();
	
		// use a try/catch block to open the input file with a FileReader
		try {
			inputBuf = new BufferedReader(new FileReader(filename));
		}
		catch (FileNotFoundException fnfe) {
			// if the file is not found, exit the program
			System.out.println("File \"" + filename + "\" not found.");
			fnfe.printStackTrace();
			return null;
		}
		// read a line from the file using a try/catch block
		try {
			line = inputBuf.readLine();
		}
		catch (IOException ioe) {
			System.out.println("IOException during readLine().");
			ioe.printStackTrace();
			return null;
		}

		while (line != null) {
			if (line.charAt(0) != '#') { // not a comment line
				try {
					String triplet[] = line.split(" ");
					configList.add(new ServerID(Integer.parseInt(triplet[0]),
												triplet[1],
												Integer.parseInt(triplet[2])));					
				}
				catch (Exception e) {
					System.out.println("Error while parsing \"" + filename +
									   "\".");
					e.printStackTrace();
					return null;
				}
			}
			// get next line
			try {
				line = inputBuf.readLine();
			}
			catch (IOException ioe) {
				System.out.println("IOException during readLine().");
				ioe.printStackTrace();
				return null;
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
			return null;
		}

		return configList;
	}
}
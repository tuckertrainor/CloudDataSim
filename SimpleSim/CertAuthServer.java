/**
 * File: CertAuthServer.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A simple server class to simulate a Certificate Authority. Accepts client
 * connections and forks CertAuthThreads to handle them.
 */

import java.net.ServerSocket;
import java.net.Socket;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

public class CertAuthServer {
	
    public static void main(String[] args) {
		int serverNumber = 0; // Default number for CA
		
		// Load server information from configuration file
		ArrayList<ServerID> serverList = loadConfig("serverConfig.txt");
		if (serverList == null) {
			System.err.println("Error loading configuration file. Exiting.");
			System.exit(-1);
		}
		else {
			System.out.println("Configuration file loaded. CA is ready at " +
							   serverList.get(serverNumber).getAddress() + ":" +
							   serverList.get(serverNumber).getPort() + ".");
		}
		
		try {
			// This is basically just listens for new client connections
			final ServerSocket serverSock = new ServerSocket(serverList.get(serverNumber).getPort());
			
			// A simple infinite loop to accept connections
			Socket sock = null;
			CertAuthThread thread = null;
			while(true) {
				// Accept an incoming connection
				sock = serverSock.accept();
				// Create a thread to handle this connection
				thread = new CertAuthThread(sock);
				thread.start();
			}
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
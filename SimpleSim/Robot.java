/**
 * File: Robot.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * Based on original code "EchoClient.java" by Adam J. Lee (adamlee@cs.pitt.edu) 
 *
 * Simple client class. This class connects to a CloudServer to send
 * text back and forth. Java message serialization is used to pass
 * Message objects around.
 */

import java.net.Socket;             // Used to connect to the server
import java.io.ObjectInputStream;   // Used to read objects sent from the server
import java.io.ObjectOutputStream;  // Used to write objects to the server
import java.io.BufferedReader;      // Needed to read from the console
import java.io.InputStreamReader;   // Needed to read from the console
import java.net.ConnectException;

import java.io.FileReader;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.ArrayList;

public class Robot {
	/**
	 * Main method.
	 *
	 * @param args - First argument specifies the server address, second
	 * argument specifies the port number
	 */
    public static void main(String[] args) {
		// Error checking for arguments
		if (args.length != 1) {
			System.err.println("Improper argument count.");
			System.err.println("Usage: java Robot <Server number to first contact>\n");
			System.exit(-1);
	    }
		
		// Load the parameters for this simulation
		loadParameters();
		
		// Load server information from server configuration file
		ArrayList<ServerID> serverList = loadConfig("serverConfig.txt");
		if (serverList == null) {
			System.err.println("Error loading configuration file. Exiting.");
			System.exit(-1);
		}
		else {
			System.out.println("Server configuration file read successfully.");
		}
		
		// Check arg[0] for proper value, range
		int primaryServer = 0;
		try {
			primaryServer = Integer.parseInt(args[0]);
			if (primaryServer < 1 || primaryServer >= serverList.size()) {
				System.err.println("Error in server number. Please check server configuration.");
				System.exit(-1);
			}
		}
		catch (Exception e) {
			System.err.println("Error parsing argument. Please use a valid integer.");
			System.err.println("Usage: java Robot <Server number to first contact>\n");
			System.exit(-1);
		}
		
		// Communicate with CloudServer through RobotThread
		try {
			
			RobotThread thread = null;
			// B 1;R 1 1,R 1 3,R 1 1,R 1 2;W 1 2,W 1 1;R 1 3;C 1;exit
//			thread = new RobotThread("B 1&R 1 1&R 1 3&R 1 1&R 1 2&C 1&exit",
//									 serverList.get(primaryServer).getAddress(),
//									 serverList.get(primaryServer).getPort());
			thread = new RobotThread("B 1;R 1 1,R 1 3,R 1 1,R 1 2;W 1 2,W 1 1;R 1 3;C 1;exit",
									 serverList.get(primaryServer).getAddress(),
									 serverList.get(primaryServer).getPort());
			thread.start();
			ThreadCounter.robotThreads++;
			thread = new RobotThread("B 2;R 2 1,R 2 3,R 2 1,R 2 2;W 2 2,W 2 1;R 2 3;C 2;exit",
									 serverList.get(primaryServer).getAddress(),
									 serverList.get(primaryServer).getPort());
			thread.start();
			ThreadCounter.robotThreads++;
			thread = new RobotThread("B 3;R 3 1,R 3 3,R 3 1,R 3 2;W 3 2,W 3 1;R 3 3;C 1;exit",
									 serverList.get(primaryServer).getAddress(),
									 serverList.get(primaryServer).getPort());
			thread.start();
			ThreadCounter.robotThreads++;
		}
		catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
		
		System.out.println("Thread count: " + ThreadCounter.robotThreads);
    }

    /**
     * Simple method to print a prompt and read a line of text.
     *
     * @return A line of text read from the console
     */
	private static String readSomeText() {
		try {
			System.out.println("Enter a line of text, or type \"EXIT\" to quit.");
			System.out.print(" > ");	
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			return in.readLine();
		}
		catch (Exception e) {
			// Uh oh...
			return "";
		}
	}
	
    /**
     * Load a file containing the parameters and applicable data for the Robot
     */
	private static void loadParameters() {
		System.out.println("loadParameters() stub");
		// load parameters file
		
		BufferedReader inputBuf = null;
		String line = null;
		// use a try/catch block to open the input file with a FileReader
		try {
			inputBuf = new BufferedReader(new FileReader("parameters.txt"));
		}
		catch (FileNotFoundException fnfe) {
			// if the file is not found, exit the program
			System.out.println("File \"parameters.txt\" not found. Exiting program.");
			fnfe.printStackTrace();
			System.exit(0);
		}
		// read a line from the dictionary file using a try/catch block
		try {
			line = inputBuf.readLine();
			System.out.println(line);
		}
		catch (IOException ioe) {
			System.out.println("IOException during readLine(). Exiting program.");
			ioe.printStackTrace();
			System.exit(0);
		}
		
		// close BufferedReader using a try/catch block
		try {
			inputBuf.close();
			
		}
		catch (IOException ioe) {
			// if exception caught, exit the program
			System.out.println("Error closing reader. Exiting program");
			ioe.printStackTrace();
			System.exit(0);
		}

	}
	/**
	 * Loads the configuration file for servers, giving Robot knowledge of
	 * server addresses as well as its own
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
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

public class Robot {
	/**
	 * Main method.
	 *
	 * @param args - First argument specifies the server address, second
	 * argument specifies the port number
	 */
    public static void main(String[] args) {
		// Error checking for arguments
		if (args.length != 2) {
			System.err.println("Not enough arguments.\n");
			System.err.println("Usage: java Robot <Server name or IP> <Port Number>\n");
			System.exit(-1);
	    }
		
		// Load the parameters for this simulation
		loadParameters();
		
		// Communicate with CloudServer through RobotThread
		try {
			RobotThread thread = null;
			thread = new RobotThread("B 1&R 1 1&R 1 3&R 1 1&R 1 2&C 1&exit", args[0], Integer.parseInt(args[1]));
			thread.start();
			ThreadCounter.robotThreads++;
			thread = new RobotThread("B 2&R 2 1&R 2 3&R 2 1&R 2 2&C 2&exit", args[0], Integer.parseInt(args[1]));
			thread.start();
			ThreadCounter.robotThreads++;
			thread = new RobotThread("B 3&R 3 1&R 3 3&R 3 1&R 3 2&C 3&exit", args[0], Integer.parseInt(args[1]));
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
}
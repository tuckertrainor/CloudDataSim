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
import java.util.Random;
import java.util.Date;

public class Robot {
	static int maxTransactions;
	static int maxQueries;
	static int minQueries;
	static int maxServers;
	static int maxDegree;
	static int latencyMin;
	static int latencyMax;
	static boolean threadSleep;
	static int verificationType;
	static float integrityCheckSuccessRate;
	static float localAuthSuccessRate;
	static float globalAuthSuccessRate;
	static int policyUpdateMin;
	static int policyUpdateMax;
	static long randomSeed;
	static Random generator;

	/**
	 * Main method.
	 *
	 * @param args - First argument specifies the server address, second
	 * argument specifies the port number
	 */
    public static void main(String[] args) {
		// Error checking for arguments
		if (args.length < 1 || args.length > 5 || args.length == 3) {
			System.err.println("Improper argument count.");
			argsError();
			System.exit(-1);
	    }
		
		// Load the parameters for this simulation
		if (loadParameters("parameters.txt")) {
			System.out.println("Parameters file read successfully.");
		}
		else {
			System.err.println("Error loading parameters file. Exiting.");
			System.exit(-1);
		}
		
		// Load server information from server configuration file
		ArrayList<ServerID> serverList = loadConfig("serverConfig.txt");
		if (serverList == null) {
			System.err.println("Error loading configuration file. Exiting.");
			System.exit(-1);
		}
		else {
			System.out.println("Server configuration file read successfully.");
		}
		
		int primaryServer = 0;
		switch (args.length) {
			case 1:
				primaryServer = getTM(serverList, args[0]);
				break;
			case 2:
				primaryServer = getTM(serverList, args[0]);
				setSeed(args[1]);
				break;
			case 4:
				primaryServer = getTM(serverList, args[0]);
				setSeed(args[1]);
				setQmin(args[2]);
				setQmax(minQueries, args[3]);
				break;
			case 5:
				primaryServer = getTM(serverList, args[0]);
				setSeed(args[1]);
				setQmin(args[2]);
				setQmax(minQueries, args[3]);
				setVT(args[4]);
				break;
			default: // We should never reach here, but just in case
				System.err.println("Default case reached in switch. Exiting.");
				System.exit(-1);
				break;
		}
		
		// Push parameters to active servers
		if (!parameterPush(serverList)) {
			System.err.println("Error pushing parameters. Exiting.");
			System.exit(-1);
		}
		
		// Build a series of transactions using parameters
		generator = new Random(randomSeed);
		TransactionData tData = new TransactionData(0, "ZERO");
		tData.setStartTime();
		tData.setEndTime(0L);
		TransactionLog.entry.add(tData);
		String newTrans;
		char prevQuery;
		int queryServer;
		int operations;
		for (int i = 1; i <= maxTransactions; i++) {
			newTrans = "B " + i;
			prevQuery = 'B';
			queryServer = 0;
			// Get random number of queries for this transaction
			operations = minQueries + generator.nextInt(maxQueries - minQueries);
			for (int j = 0; j < operations; j++) {
				String newQuery = new String();
				// make READ or WRITE
				if (generator.nextBoolean()) {
					if (prevQuery == 'R') {
						newQuery += ";R " + i; // ,
					}
					else {
						newQuery += ";R " + i;
					}
					prevQuery = 'R';
				}
				else {
					if (prevQuery == 'W') {
						newQuery += ";W " + i; // ,
					}
					else {
						newQuery += ";W " + i;
					}
					prevQuery = 'W';
				}
				// make server number
				queryServer = generator.nextInt(maxServers) + 1;
				newQuery += " " + queryServer;
				// add sequence number
				newQuery += " " + (j + 1);
				newTrans += newQuery;
			}
			newTrans += ";C " + i + ";exit";
			tData = new TransactionData(i, newTrans);
			tData.setStartTime();
			TransactionLog.entry.add(tData);
		}
		
		// Communicate with CloudServer through RobotThread
		try {
			RobotThread thread = null;
			int i = 1;
			
			while (ThreadCounter.threadCount < maxTransactions) {
				if (ThreadCounter.activeThreads < maxDegree) {
					TransactionLog.entry.get(i).setStartTime();
					thread = new RobotThread(i,
											 primaryServer,
											 TransactionLog.entry.get(i).getQuerySet(),
											 serverList.get(primaryServer).getAddress(),
											 serverList.get(primaryServer).getPort(),
											 latencyMin,
											 latencyMax,
											 threadSleep);
					thread.start();
					ThreadCounter.addNewThread();
					i++;
				}
				else {
					// Release I/O block
					Thread.yield();
				}
			}
		}
		catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
		
		// Wait for all threads to complete
		while (ThreadCounter.activeThreads != 0) {
			Thread.yield();
		}
		
		// Record output log
		if (outputLog()) {
			System.out.println("Log created.");
		}
		else {
			System.out.println("Error during log file creation.");
		}
    }
	
    /**
     * Load a file containing the parameters and applicable data for the Robot
     */
	private static boolean loadParameters(String filename) {
		BufferedReader inputBuf = null;
		String line = null;
		// use a try/catch block to open the input file with a FileReader
		try {
			inputBuf = new BufferedReader(new FileReader(filename));
		}
		catch (FileNotFoundException fnfe) {
			// if the file is not found, exit the program
			System.out.println("File \"" + filename + "\" not found. Exiting program.");
			fnfe.printStackTrace();
			return false;
		}
		
		// Read and parse the contents of the file
		try {
			line = inputBuf.readLine();
		}
		catch (IOException ioe) {
			System.out.println("IOException during readLine(). Exiting program.");
			ioe.printStackTrace();
			return false;
		}
		while (line != null) {
			if (line.charAt(0) != '#') { // not a comment line
				try {
					String tuple[] = line.split(" ");
					if (tuple[0].equals("MT")) {
						maxTransactions = Integer.parseInt(tuple[1]);
						ThreadCounter.maxThreads = maxTransactions;
					}
					else if (tuple[0].equals("QMIN")) {
						minQueries = Integer.parseInt(tuple[1]);
					}
					else if (tuple[0].equals("QMAX")) {
						maxQueries = Integer.parseInt(tuple[1]);
					}
					else if (tuple[0].equals("MS")) {
						maxServers = Integer.parseInt(tuple[1]);
					}
					else if (tuple[0].equals("MD")) {
						maxDegree = Integer.parseInt(tuple[1]);
					}
					else if (tuple[0].equals("LMIN")) {
						latencyMin = Integer.parseInt(tuple[1]);
					}
					else if (tuple[0].equals("LMAX")) {
						latencyMax = Integer.parseInt(tuple[1]);
					}
					else if (tuple[0].equals("SLEEP")) {
						threadSleep = Boolean.parseBoolean(tuple[1]);
					}
					else if (tuple[0].equals("VT")) {
						verificationType = Integer.parseInt(tuple[1]);
					}
					else if (tuple[0].equals("ICSR")) {
						integrityCheckSuccessRate = Float.parseFloat(tuple[1]);
					}
					else if (tuple[0].equals("LASR")) {
						localAuthSuccessRate = Float.parseFloat(tuple[1]);
					}
					else if (tuple[0].equals("GASR")) {
						globalAuthSuccessRate = Float.parseFloat(tuple[1]);
					}
					else if (tuple[0].equals("PMIN")) {
						policyUpdateMin = Integer.parseInt(tuple[1]);
					}
					else if (tuple[0].equals("PMAX")) {
						policyUpdateMax = Integer.parseInt(tuple[1]);
					}
					else if (tuple[0].equals("RS")) {
						randomSeed = Long.parseLong(tuple[1]);
					}
				}
				catch (Exception e) {
					System.out.println("Error while parsing \"" + filename +
									   "\".");
					e.printStackTrace();
					return false;
				}
			}
			// get next line
			try {
				line = inputBuf.readLine();
			}
			catch (IOException ioe) {
				System.out.println("IOException during readLine().");
				ioe.printStackTrace();
				return false;
			}
		}

		// close BufferedReader using a try/catch block
		try {
			inputBuf.close();
		}
		catch (IOException ioe) {
			// if exception caught, exit the program
			System.out.println("Error closing reader. Exiting program");
			ioe.printStackTrace();
			return false;
		}
		
		return true; // success
	}
	
	public static void argsError() {
		System.err.println("Usage: java Robot <TM Number> or");
		System.err.println("Usage: java Robot <TM Number> <Seed> or");
		System.err.println("Usage: java Robot <TM Number> <Seed> <QMIN> <QMAX> or");
		System.err.println("Usage: java Robot <TM Number> <Seed> <QMIN> <QMAX> <VT>\n");
	}
	
	public static int getTM(ArrayList<ServerID> _serverList, String str) {
		int number = 0;
		// Check arg for proper value, range
		try {
			number = Integer.parseInt(str);
			if (number < 1 || number >= _serverList.size()) {
				System.err.println("Error in server number. Please check server configuration.");
				System.exit(-1);
			}
		}
		catch (Exception e) {
			System.err.println("Error parsing argument for TM. Please use a valid integer.");
			argsError();
			System.exit(-1);
		}
		return number;
	}
		
	public static void setSeed(String str) {
		try {
			randomSeed = Long.parseLong(str);
		}
		catch (Exception e) {
			System.err.println("Error parsing argument for seed. Please use a valid integer.");
			argsError();
			System.exit(-1);
		}
	}
	
	public static void setQmin(String str) {
		int min = 0;
		// Check arg for proper value, range
		try {
			min = Integer.parseInt(str);
			if (min < 1) {
				System.err.println("Error in QMIN. Please set a minimum of at least 1.");
				System.exit(-1);
			}
			minQueries = min;
		}
		catch (Exception e) {
			System.err.println("Error parsing argument for QMIN. Please use a valid integer.");
			argsError();
			System.exit(-1);
		}
	}
	
	public static void setQmax(int min, String str) {
		int max = 0;
		// Check arg for proper value, range
		try {
			max = Integer.parseInt(str);
			if (max < min) {
				System.err.println("Error in QMAX. Please set a value equal to or greater than QMIN.");
				System.exit(-1);
			}
			maxQueries = max;
		}
		catch (Exception e) {
			System.err.println("Error parsing argument for QMAX. Please use a valid integer.");
			argsError();
			System.exit(-1);
		}
	}
	
	public static void setVT(String str) {
		int number = -1;
		// Check arg for proper value, range
		try {
			number = Integer.parseInt(str);
			if (number < 0 || number > 4) {
				System.err.println("Error in VT. Please set a value in the range of 0 - 4.");
				System.exit(-1);
			}
			verificationType = number;
		}
		catch (Exception e) {
			System.err.println("Error parsing argument for VT. Please use a valid integer.");
			argsError();
			System.exit(-1);
		}
	}

	public static boolean parameterPush(ArrayList<ServerID> list) {
		Socket socket;
		ObjectOutputStream output;
		ObjectInputStream input;
		for (int i = 1; i <= maxServers; i++) {
			try {
				socket = new Socket(list.get(i).getAddress(), list.get(i).getPort());
				// Set up I/O streams with the server
				output = new ObjectOutputStream(socket.getOutputStream());
				input = new ObjectInputStream(socket.getInputStream());
				
				Message msg = new Message("PARAMETERS " +
										  latencyMin + " " +
										  latencyMax + " " +
										  threadSleep + " " +
										  verificationType + " " +
										  integrityCheckSuccessRate	+ " " +
										  localAuthSuccessRate);
				output.writeObject(msg);
				msg = (Message)input.readObject();
				if (!msg.theMessage.equals("ACK")) {
					System.err.println("Error: Incorrect ACK from " + socket.getInetAddress() +
									   ":" + socket.getPort());
					System.err.println("Could not push parameters to server " + i + ". Exiting.");
					socket.close();
					return false;
				}
				else { // Success
					System.out.println("Parameters successfully pushed to server " + i + ".");
				}
				socket.close();
			}
			catch(ConnectException ce) {
				System.out.println("** Connect Exception for " + list.get(i).getAddress() +
								   ":" + list.get(i).getPort() +
								   " - could not push parameters **");
				return false;
			}
			catch(Exception e) {
				System.err.println("Error during parameters push: " + e.getMessage());
				e.printStackTrace(System.err);
				return false;
			}
		}
		return true;
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
	
    /**
     * Output a file with the results of the simulation
     */
	private static boolean outputLog() {
		FileWriter outputFile = null;
		BufferedWriter outputBuf = null;
		long fileTime = new Date().getTime();
		String filename = "Log_" + Long.toString(fileTime) + ".txt";
		boolean success = true;
		
		// Create an output stream
		try {			
			outputFile = new FileWriter(filename, true);
			// create a BufferedWriter object for the output methods
			outputBuf = new BufferedWriter(outputFile);
		}
		catch(IOException ioe) {
			System.out.println("IOException during output file creation.");
			ioe.printStackTrace();
			success = false;
		}
		
		// Write to file
		try {
			outputBuf.write("PARAMETERS:");
			outputBuf.newLine();
			outputBuf.write("MT=" + maxTransactions);
			outputBuf.newLine();
			outputBuf.write("QMIN=" + minQueries);
			outputBuf.newLine();
			outputBuf.write("QMAX=" + maxQueries);
			outputBuf.newLine();
			outputBuf.write("MS=" + maxServers);
			outputBuf.newLine();
			outputBuf.write("MD=" + maxDegree);
			outputBuf.newLine();
			outputBuf.write("LMIN=" + latencyMin);
			outputBuf.newLine();
			outputBuf.write("LMAX=" + latencyMax);
			outputBuf.newLine();
			outputBuf.write("SLEEP=" + threadSleep);
			outputBuf.newLine();
			outputBuf.write("VT=" + verificationType);
			outputBuf.newLine();
			outputBuf.write("ICSR=" + integrityCheckSuccessRate);
			outputBuf.newLine();
			outputBuf.write("LASR=" + localAuthSuccessRate);
			outputBuf.newLine();
			outputBuf.write("PMIN=" + policyUpdateMin);
			outputBuf.newLine();
			outputBuf.write("PMAX=" + policyUpdateMax);
			outputBuf.newLine();
			outputBuf.write("RS=" + randomSeed);
			outputBuf.newLine();
			if (threadSleep) {
				for (int i = 1; i <= maxTransactions; i++) {
					outputBuf.write(TransactionLog.entry.get(i).getTransNumber() + "\t" +
									TransactionLog.entry.get(i).getQuerySet() + "\t" +
									TransactionLog.entry.get(i).getStartTime() + "\t" +
									TransactionLog.entry.get(i).getEndTime() + "\t" +
									TransactionLog.entry.get(i).getDuration() + "\t" +
									TransactionLog.entry.get(i).getStatus());
					outputBuf.newLine();
				}
			}
			else { // output sleep time in data
				for (int i = 1; i <= maxTransactions; i++) {
					outputBuf.write(TransactionLog.entry.get(i).getTransNumber() + "\t" +
									TransactionLog.entry.get(i).getQuerySet() + "\t" +
									TransactionLog.entry.get(i).getStartTime() + "\t" +
									TransactionLog.entry.get(i).getEndTime() + "\t" +
									TransactionLog.entry.get(i).getDuration() + "\t" +
									TransactionLog.entry.get(i).getSleepTime() + "\t" +
									TransactionLog.entry.get(i).getStatus());
					outputBuf.newLine();
				}
			}
		}
		catch(IOException ioe) {
			System.out.println("IOException while writing to output file.");
			ioe.printStackTrace();
			success = false;
		}
		
		// Close the output stream
		try {
			outputBuf.close();
		}
		catch(IOException ioe) {
			System.out.println("IOException while closing the output file.");
			ioe.printStackTrace();
			success = false;
		}
		
		return success;
	}
}
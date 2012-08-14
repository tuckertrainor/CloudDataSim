/**
 * File: RobotThread.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * Based on original code "EchoThread.java" by Adam J. Lee (adamlee@cs.pitt.edu) 
 *
 * A thread that accepts a series of transactions from the Robot and sends them
 * to a CloudServer
 */

import java.lang.Thread;
import java.net.Socket;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.util.*;

public class RobotThread implements Runnable {
	private final int txnNumber;
	private final int coordinator;
    private final String transactions;
	private final String server;
	private final int port;
	private final int latencyMin;
	private final int latencyMax;
	private final boolean threadSleep;
	private final boolean verbose;
	private Random generator;
	private final long seed2;

	/**
	 * Constructor that sets up transaction communication
	 *
	 * @param int _txnNumber - The number of this transaction set
	 * @param int _coordinator - The server number of the transaction's
	 * coordinator
	 * @param int _transactions - A string representing the queries to be run
	 * @param String _server - The server name where the primary Transaction
	 * Manager is located
	 * @param int _port - The port number of the server
	 * @param int _lMin - Minimum simulated latency
	 * @param int _lMax - Maximum simulated latency
	 * @param boolean _threadSleep - Whether to Thread.sleep() for latency
	 * @param boolean _verbose - Whether to output each transaction status
	 */
	public RobotThread(int _txnNumber, int _coordinator, String _transactions, String _server, int _port, int _lMin, int _lMax, boolean _threadSleep, boolean _verbose, long _seed2) {
		coordinator = _coordinator;
		txnNumber = _txnNumber;
		transactions = _transactions;
		server = _server;
		port = _port;
		latencyMin = _lMin;
		latencyMax = _lMax;
		threadSleep = _threadSleep;
		verbose = _verbose;
		seed2 = _seed2;
	}

	/**
	 * run() is basically the main method of a thread. This thread
	 * simply reads Message objects off of the socket.
	 */
	public void run() {
		try {
			// Divide transaction into groups to process in chunks (i.e., all
			// contiguous READs or WRITEs)
			String queryGroups[] = transactions.split(";");
			int groupIndex = 0;

			// Connect to the specified server
			final Socket sock = new Socket(server, port);
			if (verbose) {
				System.out.println("RobotThread: Transaction " + txnNumber +
								   " connected to " + server + " on port " + port);
			}
			// Set up I/O streams with the server
			final ObjectOutputStream output = new ObjectOutputStream(sock.getOutputStream());
			final ObjectInputStream input = new ObjectInputStream(sock.getInputStream());
			
			// Seed Random for latency, pauses
			generator = new Random(seed2);

			// Set start time of transaction
			TransactionLog.entry.get(txnNumber).setStartTime(new Date().getTime());
			
			// Loop to send query qroups
			while (groupIndex < queryGroups.length) {
				Message msg = null, resp = null;

				// If about to commit, record the time
				if (queryGroups[groupIndex].charAt(0) == 'C') {
					TransactionLog.entry.get(txnNumber).setCommitStartTime(new Date().getTime());
				}
				
				// Send message after latencySleep()
				latencySleep();
				msg = new Message(queryGroups[groupIndex]);
				output.writeObject(msg);
				
				// Get response from WorkerThread
				resp = (Message)input.readObject();
				String respSplit[] = resp.theMessage.split(" ");

				if (respSplit[0].equals("ACK")) {
					Thread.yield();
				}
				else if (respSplit[0].equals("COMMIT")) { // Successful commit
					// Set the end time of the transaction
					TransactionLog.entry.get(txnNumber).setEndTime(new Date().getTime());
				}
				else if (respSplit[0].equals("ABORT")) { // Unsuccessful transaction
					TransactionLog.entry.get(txnNumber).setStatus(respSplit[0] + ": " + respSplit[1]);
					TransactionLog.entry.get(txnNumber).setEndTime(new Date().getTime());
					if (verbose) {
						System.out.println("RobotThread: Transaction " + txnNumber + " " +
										   TransactionLog.entry.get(txnNumber).getStatus());
					}
					else {
						if (txnNumber % 25 == 0) {
							System.out.print(".");
						}
					}
					break;
				}
				else if (respSplit[0].equals("FIN")) {
					// If there was not thread sleeping, get the time used by the TM
					if (!threadSleep) {
						TransactionLog.entry.get(txnNumber).addSleepTime(Integer.parseInt(respSplit[1]));
					}
					if (verbose) {
						System.out.println("RobotThread: Transaction " + txnNumber + " " +
										   TransactionLog.entry.get(txnNumber).getStatus());
					}
					else {
						if (txnNumber % 25 == 0) {
							System.out.print(".");
						}
					}
				}
				else { // Something went wrong
					System.out.println("RobotThread: Query handling error - received \"" + respSplit[0] + "\" from server.");
					break;
				}
				groupIndex++;
			}
			
			// Send message to WorkerThread to release it
			latencySleep();
			Message msg = new Message("DONE");
			output.writeObject(msg);
			
			// Close connection to server/worker thread
			sock.close();
		}
		catch (ConnectException ce) {
			System.err.println(ce.getMessage() +
							   ": Check server address and port number.");
			ce.printStackTrace(System.err);
		}
		catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
	}
	
	public void latencySleep() {
		if (latencyMax > 0) { // There is artificial latency
			int latency;
			if (latencyMax == latencyMin) { // Fixed latency value
				latency = latencyMax;
			}
			else { // Generate a random amount within range
				latency = latencyMin + generator.nextInt(latencyMax - latencyMin);
			}
			if (threadSleep) {
				try {
					// Sleep for <latency> ms
					Thread.sleep(latency);
				}
				catch(Exception e) {
					System.err.println("latencySleep() Error: " + e.getMessage());
					e.printStackTrace(System.err);
				}
			}
			else { // add int amount to log entry
				TransactionLog.entry.get(txnNumber).addSleepTime(latency);
			}
		}
	}
}
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
import java.util.Date;
import java.util.ArrayList;
import java.util.Random;

public class RobotThread extends Thread {
	private final int txnNumber;
	private final int coordinator;
    private final String transactions;
	private final String server;
	private final int port;
	private final int latencyMin;
	private final int latencyMax;
	private final boolean threadSleep;
	private ArrayList<CommitItem> commitStack = new ArrayList<CommitItem>();
	private Random generator;

	/**
	 * Constructor that sets up transaction communication
	 *
	 * @param _txnNumber - the number of this transaction set
	 * @param _transactions - A string representing the queries to be run
	 * @param _server - The server name where the primary Transaction Manager
	 * is located
	 * @param _port - The port number of the server
	 */
	public RobotThread(int _txnNumber, int _coordinator, String _transactions, String _server, int _port, int _lMin, int _lMax, boolean _threadSleep) {
		coordinator = _coordinator;
		txnNumber = _txnNumber;
		transactions = _transactions;
		server = _server;
		port = _port;
		latencyMin = _lMin;
		latencyMax = _lMax;
		threadSleep = _threadSleep;
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
			System.out.println("RobotThread: Transaction " + txnNumber +
							   " connected to " + server + " on port " + port);
			// Set up I/O streams with the server
			final ObjectOutputStream output = new ObjectOutputStream(sock.getOutputStream());
			final ObjectInputStream input = new ObjectInputStream(sock.getInputStream());
			
			// Seed Random for latency, pauses
			generator = new Random(new Date().getTime());
			
			// Loop to send query qroups
			while (groupIndex < queryGroups.length) {
				Message msg = null, resp = null;

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
				else if (respSplit[0].equals("ACS")) { // add to commitStack
					// parse data from message
					commitStack.add(new CommitItem(Integer.parseInt(respSplit[1]),
												   Integer.parseInt(respSplit[2]),
												   Integer.parseInt(respSplit[3])));
				}
				else if (respSplit[0].equals("ABORT")) {
					// Something did not validate
					TransactionLog.entry.get(txnNumber).setStatus(respSplit[0] + ": " + respSplit[1]);
					TransactionLog.entry.get(txnNumber).setEndTime(new Date().getTime());
					ThreadCounter.threadComplete(); // remove thread from active count
					System.out.println("RobotThread: Transaction " + txnNumber + " " +
									   TransactionLog.entry.get(txnNumber).getStatus());
					break;
				}
				else if (respSplit[0].equals("FIN")) {
					// Set the end time of the transaction
					TransactionLog.entry.get(txnNumber).setEndTime(new Date().getTime());
					// If there was not thread sleeping, get the time used by the TM
					if (!threadSleep) {
						TransactionLog.entry.get(txnNumber).addSleepTime(Integer.parseInt(respSplit[1]));
					}
					ThreadCounter.threadComplete(); // remove thread from active count
					System.out.println("RobotThread: Transaction " + txnNumber + " " +
									   TransactionLog.entry.get(txnNumber).getStatus());
				}
				else { // Something went wrong
					System.out.println("RobotThread: Query handling error - received \"" + respSplit[0] + "\" from server.");
					// break; // ?
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
		int latency = latencyMin + generator.nextInt(latencyMax - latencyMin);
		if (threadSleep) {
			try {
				// Sleep for a random period of time between min ms and max ms
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
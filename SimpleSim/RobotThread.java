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
	private final int transNumber;
	private final int primaryServer;
    private final String transactions;
	private final String server;
	private final int port;
	private ArrayList<Integer> commitStack = new ArrayList<Integer>();

	/**
	 * Constructor that sets up transaction communication
	 *
	 * @param _transNumber - the number of this transaction set
	 * @param _transactions - A string representing the queries to be run
	 * @param _server - The server name where the primary Transaction Manager
	 * is located
	 * @param _port - The port number of the server
	 */
	public RobotThread(int _transNumber, int _primaryServer, String _transactions, String _server, int _port) {
		primaryServer = _primaryServer;
		transNumber = _transNumber;
		transactions = _transactions;
		server = _server;
		port = _port;
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
			System.out.println("Connected to " + server + " on port " + port);
			// Set up I/O streams with the server
			final ObjectOutputStream output = new ObjectOutputStream(sock.getOutputStream());
			final ObjectInputStream input = new ObjectInputStream(sock.getInputStream());
			
			// Loop to send query qroups
			while (groupIndex < queryGroups.length) {
				Message msg = null, resp = null;

				// Send message
				msg = new Message(queryGroups[groupIndex]);
				output.writeObject(msg);
				
				// Get ACK and print
				resp = (Message)input.readObject();

				if (resp.theMessage.equals("ACK")) {
					System.out.println("RobotThread: query group processed");
				}
				else if (resp.theMessage.substring(0,3).equals("ACS")) { // add to commitStack
					// parse server number from message
					String temp[] = resp.theMessage.split(" ");
					int commitOnServer = Integer.parseInt(temp[1]);
					if (!commitStack.contains(commitOnServer)) { // add if new #
						commitStack.add(commitOnServer);
					}
				}
				else if (resp.theMessage.equals("FIN")) {
					TransactionLog.entry.get(transNumber).setEndTime(new Date().getTime());
					ThreadCounter.threadComplete(); // remove thread from active count
					System.out.println("RobotThread: transaction " + transNumber + " complete");
				}
				else { // Something went wrong
					System.out.println("RobotThread: query handling error");
					// break; // ?
				}
				groupIndex++;
				// Random pause after completing query qroup
				Thread.sleep(1000);
			}
			
			// Send message to WorkerThread to release it
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
}
/**
 * File: ContinuousThread.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A server thread to handle continuous proofs of authorization.
 * Extends the IncrementalThread class.
 */

import java.lang.Thread;
import java.net.Socket;
import java.net.ConnectException;
import java.io.*;
import java.util.*;

public class ContinuousThread extends IncrementalThread {
	
	/**
	 * Constructor that sets up the socket we'll chat over
	 *
	 * @param _socket - The socket passed in from the server
	 * @param _my_tm - The Transaction Manager that called the thread
	 */
	public ContinuousThread(Socket _socket, CloudServer _my_tm) {
		super(_socket, _my_tm);
	}
}
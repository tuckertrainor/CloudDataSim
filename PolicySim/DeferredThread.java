/**
 * File: DeferredThread.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A server thread to handle deferred proofs of authorization. Extends the
 * WorkerThread base class.
 */

import java.lang.Thread;
import java.net.Socket;
import java.net.ConnectException;
import java.io.*;
import java.util.*;

public class DeferredThread extends WorkerThread {
	
	/**
	 * Constructor that sets up the socket we'll chat over
	 *
	 * @param _socket - The socket passed in from the server
	 * @param _my_tm - The Transaction Manager that called the thread
	 */
	public DeferredThread(Socket _socket, CloudServer _my_tm) {
		super(_socket, _my_tm);
	}
}

/**
 * File: PunctualThread.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A server thread to handle punctual proofs of authorization. Extends the
 * DeferredThread base class.
 */

import java.lang.Thread;
import java.net.Socket;
import java.net.ConnectException;
import java.io.*;
import java.util.*;

public class PunctualThread extends DeferredThread {
	
	/**
	 * Constructor that sets up the socket we'll chat over
	 *
	 * @param _socket - The socket passed in from the server
	 * @param _my_tm - The Transaction Manager that called the thread
	 */
	public PunctualThread(Socket _socket, CloudServer _my_tm) {
		super(_socket, _my_tm);
	}
}

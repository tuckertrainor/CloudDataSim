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
	
	/* View consistency
	 * 1.  get freshest policy off own server
	 * 2.  perform first operation, double check local policy, perform proof, perform operation, PASS,ACK or FAIL
	 * 3.  repeat if next operation on coordinator's server, else have participant join
	 
	 * 4.  have server join (and set up update condition)
	 * 5.  coordinator sends its txn policy version and operation to server
	 * 6.  server compares its policy to coordinator's, uses freshest for proof then runs operation, returns PASS, ACK, v. or FAIL, v.
	 
	 * 7.  coordinator receives PASS, ACK, v. or FAIL, v., compares v. to own version
	 * 8.  if v. is fresher, update txn policy version, run 2PV starting on own server
	 
	 * 9.  2PV: send txn policy version to all participants, along with 2PV msg
	 *	   just received freshest from server S, send it back to him anyway? eat cost of 2-way msg?
	 * 10. each participant checks txn policy version, if < v. then sets it to v. and re-runs auths
	 * 11. sends back PASS or FAIL along with v.
	 *     should we have a different response if policy was up to date?
	 * 12. coordinator gathers policy versions again, repeats 2PV if fresher is found
	 
	 * 13. continues until PTC
	 */
}
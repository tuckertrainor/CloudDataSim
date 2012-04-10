/**
 * File: CommitItem.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A class to store transaction/sequence data of queries added to a commit stack
 */

public class CommitItem {
	private int serverNumber;
	private int transactionNumber;
	private int sequenceNumber;
	
	public CommitItem (int server, int trans, int seq) {
		serverNumber = server;
		transactionNumber = trans;
		sequenceNumber = seq;
	}
	
	public int getServer() {
		return serverNumber;
	}
	
	public int getTransaction() {
		return transactionNumber;
	}
	
	public int getSequence() {
		return sequenceNumber;
	}
}
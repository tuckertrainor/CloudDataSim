/**
 * File: CommitItem.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A class to store transaction/sequence data of queries added to a commit stack
 */

public class CommitItem {
	private int transactionNumber;
	private int sequenceNumber;
	
	public CommitItem (int trans, int seq) {
		transactionNumber = trans;
		sequenceNumber = seq;
	}
	
	public int getTransaction() {
		return transactionNumber;
	}
	
	public int getSequence() {
		return sequenceNumber;
	}
}
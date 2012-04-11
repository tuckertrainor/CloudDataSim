/**
 * File: QueryRecord.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A class to store query data for use in view consistency second chance commit.
 */

public class QueryRecord {
	private String queryType;
	private int transactionNumber;
	private int serverNumber;
	private int sequenceNumber;
	private int policyVersion;
	
	public QueryRecord (String query, int trans, int server, int seq, int policy) {
		queryType = query;
		transactionNumber = trans;
		serverNumber = server;
		sequenceNumber = seq;
		policyVersion = policy;
	}
	
	public String getQueryType() {
		return queryType;
	}
	
	public int getTransaction() {
		return transactionNumber;
	}

	public int getServer() {
		return serverNumber;
	}
	
	public int getSequence() {
		return sequenceNumber;
	}
	
	public int getPolicy() {
		return policyVersion;
	}
}
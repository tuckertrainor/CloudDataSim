/**
 * File: TransactionData.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A class to store data pertaining to a transacton - its number, its query
 * string, its start and end times, its status
 */

import java.util.Date;

public class TransactionData {
	private int transactionNumber;
	private String transaction;
	private long startTime;
	private long commitStartTime;
	private	long endTime;
	private int sleepTime;
	private String status;
	
	/**
	 * Constructor.
	 *
	 * @param _transNumber
	 * @param _querySet
	 */
	public TransactionData(int _transNumber, String _transaction) {
		transactionNumber = _transNumber;
		transaction = _transaction;
		sleepTime = 0;
		status = "COMMIT";
	}
	
	public void setStartTime() {
		startTime = new Date().getTime();
	}
	
	public void setStartTime(long _startTime) {
		startTime = _startTime;
	}
	
	public void setCommitStartTime() {
		commitStartTime = new Date().getTime();
	}
	
	public void setCommitStartTime(long _commitStartTime) {
		commitStartTime = _commitStartTime;
	}
	
	public void setEndTime() {
		endTime = new Date().getTime();
	}
	
	public void setEndTime(long _endTime) {
		endTime = _endTime;
	}
	
	public void addSleepTime(int sleepAmount) {
		sleepTime += sleepAmount;
	}
	
	public void setStatus(String _status) {
		status = _status;
	}
	
	public int getTxnNumber() {
		return transactionNumber;
	}
	
	public String getTxn() {
		return transaction;
	}
	
	public long getStartTime() {
		return startTime;
	}
	
	public long getCommitStartTime() {
		return commitStartTime;
	}
	
	public long getEndTime() {
		return endTime;
	}
	
	public long getDuration() {
		return endTime - startTime;
	}
	
	public long getTxnTime() {
		return commitStartTime - startTime;
	}
	
	public long getCommitTime() {
		return endTime - commitStartTime;
	}
	
	public int getSleepTime() {
		return sleepTime;
	}
	
	public String getStatus() {
		return status;
	}
}
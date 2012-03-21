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
	private String querySet;
	private long startTime;
	private	long endTime;
	private String status;
	
	/**
	 * Constructor.
	 *
	 * @param _transNumber
	 * @param _querySet
	 */
	public TransactionData(int _transNumber, String _querySet) {
		transactionNumber = _transNumber;
		querySet = _querySet;
		status = "OK";
	}
	
	public void setStartTime() {
		startTime = new Date().getTime();
	}
	
	public void setEndTime() {
		endTime = new Date().getTime();
	}
	
	public void setEndTime(long _endTime) {
		endTime = _endTime;
	}
	
	public void setStatus(String _status) {
		status = _status;
	}
	
	public int getTransNumber() {
		return transactionNumber;
	}
	
	public String getQuerySet() {
		return querySet;
	}
	
	public long getStartTime() {
		return startTime;
	}
	
	public long getEndTime() {
		return endTime;
	}
	
	public String getStatus() {
		return status;
	}
}
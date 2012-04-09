/**
 * File: PolicyUpdater.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A thread to handle Policy version number updating.
 */

import java.lang.Thread;
import java.util.Random;
import java.util.Date;
import java.lang.Integer;

public class PolicyUpdater extends Thread {
    private final int minSleep;
	private final int maxSleep;

	/**
	 * Constructor that sets up the thread
	 *
	 * @param _minSleep - The minimum amount of time between policy updates
	 * @param _maxSleep - The maximum amount of time between policy updates
	 */
	public PolicyUpdater(int _minSleep, int _maxSleep) {
		minSleep = _minSleep;
		maxSleep = _maxSleep;
	}

	public void run() {
		try {
			// Create a Random generator for thread sleeping
			Random refresh = new Random(new Date().getTime());
			// Sleep a random amount of time, then update policy version
			while (PolicyVersion.getCurrent() < Integer.MAX_VALUE) {
				Thread.sleep(minSleep + refresh.nextInt(maxSleep - minSleep));
				PolicyVersion.updatePolicy();
				System.out.println("Policy version updated to v. " + PolicyVersion.getCurrent());
			}
		}
		catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
	}
}
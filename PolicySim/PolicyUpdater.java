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
		
		PolicyThread thread = null;
		
		// Create and seed random number generator
		Random generator = new Random(new Date().getTime());
		
		// Start updates
		try {
			// Loop periodic update pushes
			while (policyVersion < Integer.MAX_VALUE) {
				// Sleep
				Thread.sleep(minPolicyUpdateSleep + generator.nextInt(maxPolicyUpdateSleep - minPolicyUpdateSleep));
				// Update policy version
				policyVersion++;
				System.out.println("Policy version updated to v. " + policyVersion);
				// Spread the word
				for (int i = 1; i <= maxServers; i++) {
					pushSleep = minPolicyPushSleep + generator.nextInt(maxPolicyPushSleep - minPolicyPushSleep);
					thread = new PolicyThread(policyVersion,
											  serverList.get(i).getAddress(),
											  serverList.get(i).getPort(),
											  pushSleep);
					thread.start();
				}
			}
			
			// Close and cleanup
			System.out.println("** Closing connection with " + socket.getInetAddress() +
							   ":" + socket.getPort() + " **");
			socket.close();
		}
		catch(Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}

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
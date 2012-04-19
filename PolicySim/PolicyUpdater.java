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
	private PolicyServer my_ps;

	/**
	 * Constructor that sets up the thread
	 *
	 * @param _my_ps
	 */
	public PolicyUpdater(PolicyServer _my_ps) {
		my_ps = _my_ps;
	}

	public void run() {
		PolicyThread thread = null;
		int policyVersion = 0;
		int latency = 0;
		
		// Create and seed random number generator
		Random generator = new Random(new Date().getTime());
		
		// Start updates
		try {
			// Loop periodic update pushes
			while (PolicyVersion.getCurrent() < Integer.MAX_VALUE) {
				// Sleep before updating Policy version
				Thread.sleep(my_ps.minPolicyUpdateSleep + generator.nextInt(my_ps.maxPolicyUpdateSleep - my_ps.minPolicyUpdateSleep));
				// Update policy version
				PolicyVersion.updatePolicy();
				policyVersion = PolicyVersion.getCurrent();
				System.out.println("Policy version updated to v. " + policyVersion);
				// Spread the word
				for (int i = 1; i <= my_ps.maxServers; i++) {
					latency = my_ps.latencyMin + generator.nextInt(my_ps.latencyMax - my_ps.latencyMin);
					thread = new PolicyThread(policyVersion,
											  my_ps.serverList.get(i).getAddress(),
											  my_ps.serverList.get(i).getPort(),
											  latency);
					thread.start();
				}
			}
		}
		catch(Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace(System.err);
		}
	}
}
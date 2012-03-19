/**
 * File: PolicyVersion.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * Class to handle version tracking of current policy number, as needed by the
 * Policy Server.
 */

public class PolicyVersion {
	// the starting policy version
	private static int currentPolicy = 1;
	
	public static void updatePolicy() {
		currentPolicy++;
	}

	public static int getCurrent() {
		return currentPolicy;
	}
}
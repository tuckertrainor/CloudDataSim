/**
 * File: ThreadCounter.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * Class to handle "global" counting of RobotThread instances
 */

public class ThreadCounter {
	// the number of RobotThreads that have been run so far
	public static int threadCount = 0;
	// the number of active RobotThreads
	public static int activeThreads = 0;
	// the number of threads in total to be run (from parameters file)
	public static int maxThreads = 0;
}
/**
 * File: SocketGroup.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A class to hold active socket connections
 */

import java.util.*;
import java.net.Socket;

public class SocketGroup {
	private static Hashtable<Integer, Socket> list = new Hashtable<Integer, Socket>();
	
	public static void addSocket(int serverNum, Socket socket) {
		list.put(serverNum, socket);
	}
	
	public static boolean hasSocket(int serverNum) {
		if (list.containsKey(serverNum)) {
			return true;
		}
		return false;
	}
	
	public static Socket getSocket(int serverNum) {
		return list.get(serverNum);
	}
}
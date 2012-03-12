/**
 * File: SocketGroup.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A class to hold active socket connections
 */

import java.util.*;
import java.net.Socket;

public class SocketGroup {
	private Hashtable<Integer, Socket> list = new Hashtable<Integer, Socket>();
	
	public void addSocket(int serverNum, Socket socket) {
		list.put(serverNum, socket);
	}
	
	public boolean hasSocket(int serverNum) {
		if (list.containsKey(serverNum)) {
			return true;
		}
		return false;
	}
	
	public Socket getSocket(int serverNum) {
		return list.get(serverNum);
	}
}
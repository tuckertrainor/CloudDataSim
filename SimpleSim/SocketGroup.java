/**
 * File: SocketGroup.java
 * @author: Tucker Trainor <tmt33@pitt.edu>
 *
 * A class to hold active socket connections
 */

import java.util.*;
import java.net.Socket;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

public class SocketGroup {
	private static Hashtable<Integer, SocketObj> list = new Hashtable<Integer, SocketObj>();
	
	public static void addSocketObj(int serverNum, SocketObj so) {
		list.put(serverNum, so);
	}
	
	public static boolean hasSocket(int serverNum) {
		if (list.containsKey(serverNum)) {
			return true;
		}
		return false;
	}
	
	public static SocketObj getSocket(int serverNum) {
		return list.get(serverNum);
	}
}

class SocketObj {
	public Socket socket;
	public ObjectOutputStream output;
	public ObjectInputStream input;
	
	public SocketObj(Socket s, ObjectOutputStream oos, ObjectInputStream ois) {
		socket = s;
		output = oos;
		input = ois;
	}
}
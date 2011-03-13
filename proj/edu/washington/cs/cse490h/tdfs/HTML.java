package edu.washington.cs.cse490h.tdfs;

import java.io.IOException;

import edu.washington.cs.cse490h.lib.PersistentStorageWriter;

public class HTML {

	private static String head = "<html>" + "<head>"
			+ "<script type=\"text/JavaScript\">" + "<!--"
			+ "function timedRefresh(timeoutPeriod) {"
			+ "	setTimeout(\"location.reload(true);\",timeoutPeriod);" + "}"
			+ "//   -->" + "</script>" + "</head>"
			+ "<body onload=\"JavaScript:timedRefresh(5000):\">";
	private static String tail = "</body>" + "</html>";

	public static String generateFriendList(String friendListFileContents) {
		String friendList = "<h2> Your friendlist: </h2>"
				+ friendListFileContents + "<br><br>";
		return friendList;
	}

	public static String generateFriendRequests(String requestList) {
		String friendRequests = "<h2> Your pending friend requests: </h2>"
				+ requestList + "<br><br>";
		return friendRequests;
	}

	public static String generateUserData(String username) {
		String userData = "<h2> Logged in as: " + username + " </h2><br><br>";
		return userData;
	}

	public static String generateMessages(String messages) {
		String msgData = "<h2> Your messages: </h2>" + messages + "<br><br>";
		return msgData;
	}

	public static void write(String friendData, String requestData,
			String username, String messageData, String filename, TDFSNode node) {
		String body = generateUserData(username) + generateMessages(messageData)
				+ generateFriendList(friendData)
				+ generateFriendRequests(requestData);
		try {
			PersistentStorageWriter w = node.getWriter(filename, false);
			String htmlOut =  head + "\n" + body + "\n" + tail;
			w.write(htmlOut);
			w.close();
		} catch (IOException e) {
			node.printError(e);
		}
	}
	
	public static void writeblank(TDFSNode node, String filename){
		String body = "Not logged in";
		try {
			PersistentStorageWriter w = node.getWriter(filename, false);
			String htmlOut =  head + "\n" + body + "\n" + tail;
			w.write(htmlOut);
			w.close();
		} catch (IOException e) {
			node.printError(e);
		}
	}

}

package edu.washington.cs.cse490h.tdfs;

import java.io.IOException;

public class HTML {

	private static String head = "<html>" + "<head>"
			+ "<script type=\"text/JavaScript\">" + "<!--"
			+ "function timedRefresh(timeoutPeriod) {"
			+ "	setTimeout(\"location.reload(true);\",timeoutPeriod);" + "}"
			+ "//   -->" + "</script>" + "</head>"
			+ "<body onload=\"JavaScript:timedRefresh(5000):\">";
	private static String tail = "</body>" + "</html>";

	private String body;

	public HTML(String body) {
		this.body = body;
	}

	public static String ex() {
		return new HTML("<p> refresh test</p>").toString();
	}

	public String generateFriendList(String friendListFileContents) {
		String friendList = "<h2> Your friendlist: </h2>"
				+ friendListFileContents + "<br><br>";
		return friendList;
	}

	public String generateFriendRequests(String requestList) {
		String friendRequests = "<h2> Your pending friend requests: </h2>"
				+ requestList + "<br><br>";
		return friendRequests;
	}

	public String generateUserData(String username) {
		String userData = "<h2> Logged in as: " + username + " </h2><br><br>";
		return userData;
	}

	public String generateMessages(String messages) {
		String msgData = "<h2> Your messages: </h2>" + messages + "<br><br>";
		return msgData;
	}

	public void generateBody(String friendData, String requestData,
			String username, String messageData) {
		this.body = generateUserData(username) + generateMessages(messageData)
				+ generateFriendList(friendData)
				+ generateFriendRequests(requestData);
	}

	@Override
	public String toString() {
		return head + "\n" + body + "\n" + tail;
	}

	public void write(TDFSNode node, String filename) {
		try {
			node.getWriter(filename, false).write(toString());
		} catch (IOException e) {
			node.printError(e);
		}
	}
}

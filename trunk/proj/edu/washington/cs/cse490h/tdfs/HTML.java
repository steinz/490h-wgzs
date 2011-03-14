package edu.washington.cs.cse490h.tdfs;

import java.io.IOException;

import edu.washington.cs.cse490h.lib.PersistentStorageWriter;

public class HTML {

	private static String head = "<html>" + "<head>"
			+ "<meta http-equiv=\"refresh\" content=\"1\"/>";
	private static String tail = "</body>" + "</html>";

	public static String generateFriendList(String friendListFileContents) {
		friendListFileContents = generateTags(friendListFileContents);
		String friendList = "<h2> Your friendlist: </h2>"
				+ friendListFileContents + "<br><br>";
		return friendList;
	}

	public static String generateFriendRequests(String requestList) {
		requestList = generateTags(requestList);
		String friendRequests = "<h2> Your pending friend requests: </h2>"
				+ requestList;
		return friendRequests;
	}

	public static String generateUserData(String username) {
		String userData = "<h2> Logged in as: " + username + " </h2>";
		return userData;
	}

	public static String generateMessages(String messages) {
		String tagged = generateTags(messages);
		String msgData = "<h2> Your messages: </h2>" + tagged;
		return msgData;
	}
	
	public static String generateTags(String data){
		String[] msgs = data.split(FBCommands.fileDelim);
		StringBuilder tagged = new StringBuilder();
		for (String msg : msgs) {
			tagged.append("<p>");
			tagged.append(msg);
			tagged.append("</p>");
		}
		return tagged.toString();
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

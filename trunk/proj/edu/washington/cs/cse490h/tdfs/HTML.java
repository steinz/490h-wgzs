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

    public String generateFriendList(String friendListFile){
    	String friendList = "<h2> Your friendlist: </h2>" 
    		+ friendListFile;
    	return friendList;
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

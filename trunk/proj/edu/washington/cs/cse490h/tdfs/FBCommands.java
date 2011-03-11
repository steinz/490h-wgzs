package edu.washington.cs.cse490h.tdfs;

public class FBCommands {
	
	private TDFSNode node;
	private String currentUserName = "";
	private String friendList = "";
	private String messageFile = "";
	private String requestList = "";
	
	
	public FBCommands(TDFSNode node){
		this.node = node;
	}
	
	public void login(String username){
		this.currentUserName = username;
		this.friendList = username + "_friendlist";
		this.messageFile = username + "_messageFile";
		this.requestList = username + "_requestList";
	}
	
	public boolean loggedIn(){
		return (this.currentUserName.equals(""));
	}
	
	public void logout(){
		if (this.currentUserName.equals("")){
			// Error? Or just warn user
		}
		this.currentUserName = "";		
		this.friendList = "";
		this.messageFile = "";
		this.requestList = "";
		// TODO: High: Stop listening to all files associated with username
	}
	
	public void sendFriendRequest(String friendName){
		if (!loggedIn())
			return;
		String msg = "REQUEST" + Proposal.packetDelimiter + this.currentUserName;
		
		node.append(this.requestList, msg, null); // TODO: High: Abortcommands?
	}
	
	public void checkFriendRequests(String username){
		if (!loggedIn())
			return;
		node.get(this.friendList, abortCommands);
		
		// TODO: Parse friend list, return as set of pending requests?
	}
	
	public void acceptFriend(String friendName){
		String msg = "ACCEPT" + Proposal.packetDelimiter + friendName;
		
		node.append(this.requestList, msg, null);
		node.append(this.friendList, friendName, null);
	}
	
	public void rejectFriend(String friendName){
		String msg = "REJECT" + Proposal.packetDelimiter + friendName;
		node.append(this.requestList, msg, null);
	}
	
	public void postMessage(String message){
		if (!loggedIn())
			return;
		node.append(this.messageFile, message, null);
	}
	
	public String readMessages(){
		String messages = "";
		
		if (!loggedIn())
			return "";
		
		node.get(this.messageFile, abortCommands);
		
		return messages;
	}
	
	
}

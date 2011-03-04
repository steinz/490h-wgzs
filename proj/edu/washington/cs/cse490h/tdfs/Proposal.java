package edu.washington.cs.cse490h.tdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import edu.washington.cs.cse490h.lib.Utility;

public class Proposal {

	static int HEADER_SIZE = 8;
	static String packetDelimiter = " ";
	public String filename;
	public LogEntry operation;
	public Integer operationNumber;
	public Integer proposalNumber;

	public Proposal(LogEntry op, String filename, int operationNumber,
			int proposalNumber) {
		this.filename = filename;
		this.operationNumber = operationNumber;
		this.proposalNumber = proposalNumber;
		this.operation = op;
	}

	public Proposal(byte[] buf) {
		unpack(buf);
	}

	public byte[] pack() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		ByteBuffer operationBuf = ByteBuffer.allocate(4);
		ByteBuffer proposalBuf = ByteBuffer.allocate(4);
		proposalBuf.putInt(proposalNumber);
		operationBuf.putInt(operationNumber);
		try{
			out.write(operationBuf.array());
			out.write(proposalBuf.array());
		} catch (IOException e){
			throw new RuntimeException();
		}
		
		byte[] filename = Utility.stringToByteArray(this.filename);
		byte[] delim = Utility.stringToByteArray(packetDelimiter);
		byte[] op = this.operation.pack();


		try {
			out.write(filename);
			out.write(delim);
			out.write(op);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return out.toByteArray();

	}

	private void unpack(byte[] packet) {
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(
				packet));

		try {
			this.operationNumber = in.readInt();
			this.proposalNumber = in.readInt();

			byte[] buf = new byte[packet.length - HEADER_SIZE];
			//in.read(buf, HEADER_SIZE, buf.length);
			in.read(buf);

			String rest = Utility.byteArrayToString(buf);
			String[] splitArray = rest.split(packetDelimiter);
			
			this.filename = splitArray[0];

			StringBuilder sb = new StringBuilder();
			for (int i = 1; i < splitArray.length; i++){
				sb.append(splitArray[i]);
				sb.append(packetDelimiter);
			}
			
			byte[] operationBuf = Utility.stringToByteArray(sb.toString().trim());
			this.operation = LogEntry.unpack(operationBuf);

		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}
}

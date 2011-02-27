package edu.washington.cs.cse490h.tdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;

import edu.washington.cs.cse490h.lib.Utility;

public class Proposal {

	static int HEADER_SIZE = 8;
	static String packetDelimiter = " ";
	public String filename;
	public Operation operation;
	public Integer operationNumber;
	public Integer proposalNumber;

	public Proposal(Operation op, String filename, int operationNumber,
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

		byte[] filename = Utility.stringToByteArray(this.filename);
		byte[] delim = Utility.stringToByteArray(packetDelimiter);
		byte[] op = this.operation.pack();

		out.write(operationNumber);
		out.write(proposalNumber);
		try {
			out.write(filename);
			out.write(delim);
			out.write(op);
		} catch (IOException e) {
			// TODO: Do something - can't print error because no node
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
			in.read(buf, HEADER_SIZE, packet.length);

			String rest = Utility.byteArrayToString(buf);
			String[] splitArray = rest.split(packetDelimiter);
			this.filename = splitArray[0];

			byte[] operationBuf = Utility.stringToByteArray(splitArray[1]);
			this.operation = Operation.unpack(operationBuf);

		} catch (IOException e) {
			// TODO: Do something
		}

	}
}
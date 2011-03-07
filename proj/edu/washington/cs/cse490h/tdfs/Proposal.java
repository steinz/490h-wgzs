package edu.washington.cs.cse490h.tdfs;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Scanner;

import edu.washington.cs.cse490h.lib.Utility;

public class Proposal {
	static final int HEADER_SIZE = 12;
	static final String packetDelimiter = " ";
	public String filename;
	public LogEntry operation;
	public int operationNumber;
	public int proposalNumber;
	
	public int originalProposal;
	
	public Proposal(LogEntry op, String filename, int operationNumber,
			int proposalNumber) {
		this.filename = filename;
		this.operationNumber = operationNumber;
		this.proposalNumber = proposalNumber;
		this.operation = op;
		this.originalProposal = -1;
	}

	public Proposal(byte[] buf) {
		unpack(buf);
	}

	public byte[] pack() {
		ByteArrayOutputStream out = new ByteArrayOutputStream();

		ByteBuffer operationBuf = ByteBuffer.allocate(4);
		ByteBuffer proposalBuf = ByteBuffer.allocate(4);
		ByteBuffer originalBuf = ByteBuffer.allocate(4);
		proposalBuf.putInt(proposalNumber);
		operationBuf.putInt(operationNumber);
		originalBuf.putInt(originalProposal); 
		try {
			out.write(operationBuf.array());
			out.write(proposalBuf.array());
			out.write(originalBuf.array());
		} catch (IOException e) {
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
			this.originalProposal = in.readInt();

			Scanner s = new Scanner(in);
			s.useDelimiter(packetDelimiter);
			this.filename = s.next();

			int bytesRead = HEADER_SIZE + this.filename.length()
					+ packetDelimiter.length();
			byte[] operationBuf = new byte[packet.length - bytesRead];
			in.reset();
			in.skip(bytesRead);

			int rest = in.read(operationBuf);
			if (rest != packet.length - bytesRead) {
				throw new RuntimeException("proposal unpack read " + rest
						+ " bytes, expected " + (packet.length - bytesRead));
			}

			this.operation = LogEntry.unpack(operationBuf);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}
}

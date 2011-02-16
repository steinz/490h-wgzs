import java.io.IOException;

import edu.washington.cs.cse490h.lib.Node;
import edu.washington.cs.cse490h.lib.Simulator;

public class SimulatorStub extends Simulator {

	public SimulatorStub(Class<? extends Node> nodeImpl, Long seed,
			String replayOutputFilename, String replayInputFilename)
			throws IllegalArgumentException, IOException {
		super(nodeImpl, seed, replayOutputFilename, replayInputFilename);
	}

	@Override
	protected void checkWriteCrash(Node n, String description) {
		return;
	}

	@Override
	public void logEventWithNodeField(Node node, String eventStr) {
	}

}
package pt.uminho.haslab.safecloudclient.shareclient.benchmarks;

import java.io.IOException;
import java.math.BigInteger;

public class PutsPerSecond implements Benchmark {

	public class ShareClient extends Thread {

		private final BigInteger key;
		private final BenchClient client;

		public ShareClient(BenchClient client, BigInteger key)
				throws IOException {
			this.client = client;
			this.key = key;
		}

		public void run() {

		}

	}
	public void runBenchmark() throws Exception {
		throw new UnsupportedOperationException("Not supported yet.");
	}

}

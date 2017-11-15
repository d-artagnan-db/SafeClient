package pt.uminho.haslab.safeclient.shareclient.benchmarks;

import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

import java.io.IOException;

public class Main {
	public static void main(String[] args) throws IOException,
			InvalidNumberOfBits, Exception {

		// String configFile = args[0];
		// int nTests = Integer.parseInt(args[1]);
		// String resultsPath = args[2];
		// int nValues = Integer.parseInt(args[3]);
		// String configFile = "hbase-client.xml";
		// int ntests = 10;
		// int nvalues = 10;

		/*
		 * List<BigInteger> values = ValuesGenerator
		 * .microbenchMarkValueGenerator(nValues);
		 */

		// BenchClient client = new BaselineClient(configFile);
		// Benchmark bench = new CreateTable(nTests, resultsPath, client);
		// TestClient testClient = new ShareClient();
		// testClient.startCluster();
		System.out.println("Going to launch benchmark");
		ConcurrentGetBench bench = new ConcurrentGetBench(1, 1, 10);
		bench.runBenchmark();
		System.out.println("Going to stop cluster");
		// testClient.stopCluster();

	}
}

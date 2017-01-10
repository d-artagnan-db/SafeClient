package pt.uminho.haslab.safecloudclient.shareclient.benchmarks;

import java.io.IOException;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

public class Main {
	public static void main(String[] args) throws IOException,
			InvalidNumberOfBits, Exception {

		String configFile = args[0];
		int nTests = Integer.parseInt(args[1]);
		String resultsPath = args[2];
		int nValues = Integer.parseInt(args[3]);
		/*
		 * List<BigInteger> values = ValuesGenerator
		 * .microbenchMarkValueGenerator(nValues);
		 */

		// BenchClient client = new BaselineClient(configFile);
		// Benchmark bench = new CreateTable(nTests, resultsPath, client);
		ConcurrentGetBench bench = new ConcurrentGetBench(10, 20, 10);
		bench.runBenchmark();
	}
}

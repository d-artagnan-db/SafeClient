package pt.uminho.haslab.safecloudclient.shareclient.benchmarks;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import pt.uminho.haslab.safecloudclient.shareclient.SharedClientConfiguration;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

public class GeneralBenchmark implements Benchmark {

	protected static final String TABLENAME = "benchmark";
	protected static final String COLUMNFAM = "values";

	private final int nTests;
	private final List<BigInteger> values;
	private final List<Long> putLatency;
	private final List<Long> getLatency;
	private final List<Long> createTableLatency;
	private final String resultsPath;
	private final BenchClient client;

	public GeneralBenchmark(int nTests, List<BigInteger> values,
			String resultsPath, BenchClient client) {
		this.nTests = nTests;
		this.values = values;
		putLatency = new ArrayList<Long>();
		getLatency = new ArrayList<Long>();
		this.client = client;
		this.resultsPath = resultsPath;
		createTableLatency = new ArrayList<Long>();

	}

	public void runBenchmark() throws IOException, InvalidNumberOfBits,
			Exception {
		client.startCluster();
		TableName tbname = TableName.valueOf(TABLENAME);
		HTableDescriptor table = new HTableDescriptor(tbname);
		HColumnDescriptor family = new HColumnDescriptor(COLUMNFAM);
		table.addFamily(family);
		Configuration conf = new Configuration();
		conf.addResource("hbase-site.xml");

		byte[] cf = "columns".getBytes();
		byte[] cq = "shareKey".getBytes();
		System.out.println("Going to put Values");
		for (int i = 0; i < nTests; i++) {
			BigInteger key = BigInteger.ZERO;
			long init = System.nanoTime();
			client.createTable(table);
			long end = System.nanoTime();
			this.createTableLatency.add(end - init);

			HTableInterface tableInt = client.getTableInterface(TABLENAME);

			for (BigInteger value : values) {
				Put put = new Put(key.toByteArray());
				put.add(cf, cq, value.toByteArray());
				init = System.nanoTime();
				tableInt.put(put);
				end = System.nanoTime();
				putLatency.add(end - init);
				key = key.add(BigInteger.ONE);
			}
			tableInt.close();
			if (i < nTests - 1) {
				client.deleteTable(TABLENAME);
			}
		}

		HTableInterface tableInt = client.getTableInterface(TABLENAME);

		Random randomGenerator = new Random();
		System.out.println("Going to get Values");

		for (int i = 0; i < nTests; i++) {
			int randomIndex = randomGenerator.nextInt(values.size());

			byte[] row = BigInteger.valueOf(randomIndex).toByteArray();
			Get get = new Get(row);
			long init = System.nanoTime();
			tableInt.get(get);
			long end = System.nanoTime();
			getLatency.add(end - init);
		}

		// WARN: ONLY CALL THIS FUNCTION AT THE END OF THE CLIENT.
		client.closeClientConnection();
		tableInt.close();
		client.stopCluster();
		System.out.println("Going to write results");
		// Write put latency
		File fout = new File(resultsPath + "/" + "put.txt");
		FileOutputStream fos = new FileOutputStream(fout);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

		for (Long latency : putLatency) {
			bw.write("" + latency);
			bw.newLine();
		}
		bw.close();

		// Write Get Latency
		fout = new File(resultsPath + "/" + "get.txt");
		fos = new FileOutputStream(fout);
		bw = new BufferedWriter(new OutputStreamWriter(fos));
		for (Long latency : getLatency) {
			bw.write("" + latency);
			bw.newLine();
		}
		bw.close();

		// Write create latency
		fout = new File(resultsPath + "/" + "create.txt");
		fos = new FileOutputStream(fout);
		bw = new BufferedWriter(new OutputStreamWriter(fos));
		for (Long latency : createTableLatency) {
			bw.write("" + latency);
			bw.newLine();
		}
		bw.close();

	}

}

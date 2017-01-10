package pt.uminho.haslab.safecloudclient.shareclient.benchmarks;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

public class CreateTable implements Benchmark {

	protected static final String TABLENAME = "benchmark";
	protected static final String COLUMNFAM = "values";

	private final int nTests;

	private final String resultsPath;

	private final List<Long> createLatency;

	private final BenchClient client;

	public CreateTable(int nTests, String resultsPath, BenchClient client) {

		this.nTests = nTests;
		this.resultsPath = resultsPath;
		createLatency = new ArrayList<Long>();
		this.client = client;
	}

	public void runBenchmark() throws Exception {

		client.startCluster();

		TableName tbname = TableName.valueOf(TABLENAME);
		HTableDescriptor table = new HTableDescriptor(tbname);
		HColumnDescriptor family = new HColumnDescriptor(COLUMNFAM);
		table.addFamily(family);
		System.out.println("Going to Start benchmark");
		for (int i = 0; i < nTests; i++) {
			System.out.println("Going for test " + i);
			long init = System.nanoTime();
			client.createTable(table);
			long end = System.nanoTime();
			createLatency.add(end - init);

			client.deleteTable(TABLENAME);

		}
		client.closeClientConnection();
		client.stopCluster();
		String res_path = resultsPath + "/" + nTests + ".txt";

		System.out.println("Going to write results to " + res_path);
		File fout = new File(res_path);
		FileOutputStream fos = new FileOutputStream(fout);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));

		for (Long latency : createLatency) {
			bw.write("" + latency);
			bw.newLine();
		}
		bw.close();
	}

}

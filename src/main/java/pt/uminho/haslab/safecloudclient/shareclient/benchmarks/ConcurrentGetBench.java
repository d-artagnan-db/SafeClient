package pt.uminho.haslab.safecloudclient.shareclient.benchmarks;

import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

public class ConcurrentGetBench {

	protected static final String TABLENAME = "benchmark";
	protected static final String COLUMNFAM = "values";
	protected byte[] cf = "columns".getBytes();
	protected byte[] cq = "shareKey".getBytes();
	protected final int nTests;
	protected final int nClients;
	protected final int totalValuesPerClient;

	public ConcurrentGetBench(int nTests, int nClients, int totalValuesPerClient) {

		this.nTests = nTests;
		this.nClients = nClients;
		this.totalValuesPerClient = totalValuesPerClient;

	}

	public class ConcurrentClient extends Thread implements BenchClient {

		private final ShareBench client;
		private final List<BigInteger> clientIdentifiers;
		private final List<BigInteger> clientValues;
		private final List<Long> times;

		public ConcurrentClient(String resources, List<BigInteger> identifiers,
				List<BigInteger> values) throws IOException {

			client = new ShareBench(resources);
			this.clientIdentifiers = identifiers;
			this.clientValues = values;
			times = new ArrayList<Long>();

		}

		public List<Long> getTimes() {
			return times;
		}

		public void startCluster() throws Exception {
			client.startCluster();
		}

		public void stopCluster() throws Exception {
			client.stopCluster();
		}

		public void createTable(HTableDescriptor table) throws Exception {
			client.createTable(table);
		}

		public void deleteTable(String tableName) throws Exception {
			client.deleteTable(tableName);
		}

		public void closeClientConnection() throws Exception {
			client.closeClientConnection();
		}

		public HTableInterface getTableInterface(String tableName)
				throws Exception {
			return client.getTableInterface(tableName);
		}

		public void startClient() {
			this.start();
		}

		public void stopClient() throws InterruptedException {
			this.join();
		}

		@Override
		public void run() {
			System.out.println("Starting execution thread");
			try {
				HTableInterface table = client.getTableInterface(TABLENAME);
				for (int i = 0; i < nTests; i++) {
					byte[] row = clientIdentifiers.get(i).toByteArray();

					System.out.println("Going to do get to "
							+ new BigInteger(row));

					// BigInteger val = clientValues.get(i);

					long start = System.nanoTime();
					Get get = new Get(row);
					table.get(get);
					long end = System.nanoTime();
					times.add(end - start);

					// BigInteger returnedValue = new
					// BigInteger(res.getValue(cf,
					// cq));
					// if (!val.equals(returnedValue)) {
					// throw new IllegalStateException("values not equal");
					// }

				}
			} catch (IOException ex) {
				System.out.println(ex);
				throw new IllegalStateException(ex);
			} catch (InvalidNumberOfBits ex) {
				System.out.println(ex);
				throw new IllegalStateException(ex);
			}

		}
	}

	private void createTable(BenchClient client) throws Exception {
		TableName tbname = TableName.valueOf(TABLENAME);
		HTableDescriptor table = new HTableDescriptor(tbname);
		HColumnDescriptor family = new HColumnDescriptor(COLUMNFAM);
		table.addFamily(family);
		client.createTable(table);
	}

	public void runBenchmark() throws Exception {
		List<ConcurrentClient> clients = new ArrayList<ConcurrentClient>();
		List<BigInteger> values = new ArrayList<BigInteger>();
		List<BigInteger> identifiers = new ArrayList<BigInteger>();
		Random random = new Random();
		String resource = "hbase-client.xml";
		for (int i = 0; i < nClients; i++) {

			List<BigInteger> cliVals = new ArrayList<BigInteger>();
			List<BigInteger> cliIdents = new ArrayList<BigInteger>();

			for (int j = 0; j < totalValuesPerClient; j++) {
				BigInteger ident = BigInteger.valueOf(i
						* (totalValuesPerClient) + j);
				cliIdents.add(ident);
				BigInteger value = new BigInteger(62, random);
				cliVals.add(value);
				values.add(value);
				identifiers.add(ident);
			}

			clients.add(new ConcurrentClient(resource, cliIdents, cliVals));

		}

		ConcurrentClient auxCli = clients.get(0);

		createTable(auxCli);
		HTableInterface table = auxCli.getTableInterface(TABLENAME);

		for (int i = 0; i < values.size(); i++) {

			byte[] identifier = identifiers.get(i).toByteArray();
			byte[] value = values.get(i).toByteArray();

			Put put = new Put(identifier);
			put.add(cf, cq, value);
			table.put(put);
			System.out.println("Going to put value "
					+ new BigInteger(identifier) + " -- "
					+ new BigInteger(value));

		}

		for (ConcurrentClient client : clients) {

			((Thread) client).start();
		}

		for (ConcurrentClient client : clients) {
			((Thread) client).join();
		}

		PrintWriter writer = new PrintWriter("results/putLatency.txt", "UTF-8");;
		for (ConcurrentClient client : clients) {
			for (long time : client.getTimes()) {
				writer.println(time);
			}
		}

		writer.close();
	}
}

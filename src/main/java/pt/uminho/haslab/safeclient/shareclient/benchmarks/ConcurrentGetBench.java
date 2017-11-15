package pt.uminho.haslab.safeclient.shareclient.benchmarks;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ConcurrentGetBench {

	protected static final String TABLENAME = "benchmark";
	protected final int nTests;
	protected final int nClients;
	protected final int totalValuesPerClient;

	public ConcurrentGetBench(int nTests, int nClients, int totalValuesPerClient) {

		this.nTests = nTests;
		this.nClients = nClients;
		this.totalValuesPerClient = totalValuesPerClient;

	}

    public TableSchema getTableSchema() {
        throw new IllegalStateException("Schema not defined");
    }

    public TableSchema getSchema() {
        throw new UnsupportedOperationException("Schema not defined.");
    }

    private void createTable(BenchClient client) throws Exception {
        TableName tbname = TableName.valueOf(TABLENAME);
        HTableDescriptor table = new HTableDescriptor(tbname);
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
        HTableInterface table = auxCli.getTableInterface(TABLENAME, getSchema());

        for (int i = 0; i < values.size(); i++) {

            byte[] identifier = identifiers.get(i).toByteArray();
            byte[] value = values.get(i).toByteArray();

            Put put = new Put(identifier);
            //throw new IllegalStateException("Columns not defined");
            //put.add(cf, cq, value);
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
        System.out.println("Going to write client results");
        PrintWriter writer = new PrintWriter("results/putLatency.txt", "UTF-8");
        ;
        for (ConcurrentClient client : clients) {
            for (long time : client.getTimes()) {
                writer.println(time);
            }
        }

        writer.close();
        System.out.println("Client results writen");

    }

    public class ConcurrentClient extends Thread implements BenchClient {

		private final ShareBench client;
		private final List<BigInteger> clientIdentifiers;
		private final List<BigInteger> clientValues;
		private final List<Long> times;

		public ConcurrentClient(String resources, List<BigInteger> identifiers,
				List<BigInteger> values) throws IOException, Exception {

			client = new ShareBench(resources);
			// client = new ShareBenchTest();
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

        public HTableInterface getTableInterface(String tableName, TableSchema schema)
                throws Exception {
            return client.getTableInterface(tableName, schema);
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
                HTableInterface table = client.getTableInterface(TABLENAME, getTableSchema());
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
			System.out.println("going to exit client thread");
		}
	}
}

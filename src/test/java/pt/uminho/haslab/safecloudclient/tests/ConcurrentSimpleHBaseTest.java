package pt.uminho.haslab.safecloudclient.tests;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.safecloudclient.clients.ShareClient;
import pt.uminho.haslab.safecloudclient.clients.TestClient;
import pt.uminho.haslab.safecloudclient.shareclient.SharedTable;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.testingutils.ValuesGenerator;

@RunWith(Parameterized.class)
public abstract class ConcurrentSimpleHBaseTest {

	protected static final int nThreads = 1;
	protected static final int nValues = 10;

	protected final String tableName = "TestPutGet";

	protected final String columnDescriptor = "values";

	protected final List<BigInteger> testingValues;

	// Each player will store a set of rows with unique identifiers.
	protected final List<List<BigInteger>> identifiers;

	protected final TestClient testClient;

	protected final CountDownLatch startSignal;

	@Parameterized.Parameters
	public static Collection valueGenerator() {
		return ValuesGenerator.concurrentPutGetGenerator(nValues, nThreads);
	}

	public ConcurrentSimpleHBaseTest(List<BigInteger> testingValues,
			List<List<BigInteger>> testingIdentifiers) throws Exception {

		this.testingValues = testingValues;
		this.identifiers = testingIdentifiers;
		testClient = new ShareClient();
		startSignal = new CountDownLatch(nThreads);
	}

	protected void createTestTable(TestClient client)
			throws ZooKeeperConnectionException, IOException, Exception {
		TableName tbname = TableName.valueOf(tableName);
		HTableDescriptor table = new HTableDescriptor(tbname);
		HColumnDescriptor family = new HColumnDescriptor(columnDescriptor);
		table.addFamily(family);
		client.createTestTable(table);

	}

	protected abstract class ConcurrentClient extends Thread {
		protected final List<BigInteger> clientTestingValues;
		protected final List<BigInteger> clientIdentifiers;
		protected final byte[] cf;
		protected final byte[] cq;
		protected final HTableInterface table;
		protected boolean testPassed;

		public ConcurrentClient(String resource,
				List<BigInteger> clientTestingValues,
				List<BigInteger> clientIdentifiers, String tableName,
				byte[] cf, byte[] cq) throws IOException, InvalidNumberOfBits {
			Configuration conf = new Configuration();
			conf.addResource(resource);
			this.clientTestingValues = testingValues;
			this.clientIdentifiers = clientIdentifiers;
			this.cf = cf;
			this.cq = cq;
			this.table = new SharedTable(conf, tableName);
			testPassed = true;

		}

		public boolean getTestPassed() {
			return testPassed;
		}

		private void fillTable() throws Exception {
			// System.out.println("vallz " + clientTestingValues.size());
			// System.out.println("identz " + clientIdentifiers.size());
			Assert.assertEquals(true, testClient.checkTableExists(tableName));
			for (int i = 0; i < clientIdentifiers.size(); i++) {
				byte[] identifier = clientIdentifiers.get(i).toByteArray();
				byte[] value = clientTestingValues.get(i).toByteArray();
				BigInteger rowID = new BigInteger(identifier);
				BigInteger val = new BigInteger(value);
				Put put = new Put(identifier);
				put.add(cf, cq, value);
				table.put(put);
				// System.out.println("Row id " + rowID + " has inserted value "
				// + val);

			}
			startSignal.countDown();
			startSignal.await();
		}

		protected abstract void testExecution();

		@Override
		public void run() {

			try {
				fillTable();
				testExecution();
			} catch (Exception ex) {
				System.out.println(ex);
				throw new IllegalStateException(ex);
			}

		}

	}

	protected abstract Thread getExecutionThread(int i);

	@Test
	public void testBoot() throws Exception {
		// System.out.println("Booting test");
		testClient.startCluster();
		createTestTable(testClient);
		assertEquals(true, testClient.checkTableExists(tableName));
		List<Thread> threads = new ArrayList<Thread>();

		for (int i = 0; i < nThreads; i++) {
			Thread t = getExecutionThread(i);
			threads.add(t);
		}

		for (Thread t : threads) {
			t.start();
		}

		for (Thread t : threads) {
			t.join();
		}

		for (Thread t : threads) {
			Assert.assertEquals(true, ((ConcurrentClient) t).getTestPassed());
			// Assert.assertEquals(true, false);
		}

		testClient.stopCluster();

	}
}

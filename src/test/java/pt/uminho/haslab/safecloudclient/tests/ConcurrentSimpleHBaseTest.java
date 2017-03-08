package pt.uminho.haslab.safecloudclient.tests;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.testingutils.ValuesGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import pt.uminho.haslab.safecloudclient.clients.ShareClient;
import pt.uminho.haslab.safecloudclient.clients.TestClient;

@RunWith(Parameterized.class)
public abstract class ConcurrentSimpleHBaseTest {

	protected static int nThreads = 10;
	protected static int nValues = 10;
	static final Log LOG = LogFactory.getLog(ConcurrentSimpleHBaseTest.class
			.getName());
	protected final String tableName = "TestPutGet";

	protected final String columnDescriptor = "values";

	protected final List<BigInteger> testingValues;

	// Each player will store a set of rows with unique identifiers.
	protected final List<List<BigInteger>> identifiers;

	protected final TestClient testClient;

	protected final CountDownLatch startSignal;

	public ConcurrentSimpleHBaseTest(List<BigInteger> testingValues,
			List<List<BigInteger>> testingIdentifiers) {
		this.testingValues = testingValues;
		this.identifiers = testingIdentifiers;
		testClient = new ShareClient();
		startSignal = new CountDownLatch(nThreads);
	}

	@Parameterized.Parameters
	public static Collection valueGenerator() {
		return ValuesGenerator.concurrentPutGetGenerator(nValues, nThreads);
	}

	protected void createTestTable() throws ZooKeeperConnectionException,
			IOException, Exception {
		TableName tbname = TableName.valueOf(tableName);
		HTableDescriptor table = new HTableDescriptor(tbname);
		HColumnDescriptor family = new HColumnDescriptor(columnDescriptor);
		table.addFamily(family);
		testClient.createTestTable(table);

	}

	protected abstract Thread getExecutionThread(int i);

	@Test
	public void testBoot() throws Exception {
		testClient.startCluster();
		createTestTable();
		assertEquals(true, testClient.checkTableExists(tableName));
		List<Thread> threads = new ArrayList<Thread>();

		for (int i = 0; i < nThreads; i++) {
			LOG.debug("Creating execution thread " + i);
			Thread t = getExecutionThread(i);
			threads.add(t);
		}

		for (Thread t : threads) {
			LOG.debug("Going to launch client thread");
			t.start();
		}

		for (Thread t : threads) {
			LOG.debug("Going to join client thread");
			t.join();
		}

		for (Thread t : threads) {
			LOG.debug("Going to check result of client thread");
			Assert.assertEquals(true, ((ConcurrentClient) t).getTestPassed());
		}

		testClient.stopCluster();

	}

	protected abstract class ConcurrentClient extends Thread {
		protected final List<BigInteger> clientIdentifiers;
		protected final byte[] cf;
		protected final byte[] cq;
		protected final HTableInterface table;
		protected final int clientID;
		protected boolean testPassed;

		public ConcurrentClient(String resource, int id, String tableName,
				byte[] cf, byte[] cq) throws IOException, InvalidNumberOfBits,
				Exception {
			Configuration conf = new Configuration();
			conf.addResource(resource);
			this.clientIdentifiers = identifiers.get(id);
			this.clientID = id;
			this.cf = cf;
			this.cq = cq;
			this.table = testClient.createTableInterface(tableName);
			testPassed = true;

		}

		public boolean getTestPassed() {
			return testPassed;
		}

		private void fillTable() {
			try {
				Assert.assertEquals(true,
						testClient.checkTableExists(tableName));
				for (int i = 0; i < clientIdentifiers.size(); i++) {
					byte[] identifier = clientIdentifiers.get(i).toByteArray();
					byte[] value = testingValues.get(i).toByteArray();
					Put put = new Put(identifier);
					put.add(cf, cq, value);
					table.put(put);

				}
				startSignal.countDown();
				startSignal.await();
			} catch (Exception ex) {
				LOG.error(ex);
				throw new IllegalArgumentException(ex);
			}
		}

		protected abstract void testExecution();

		@Override
		public void run() {
			fillTable();
			testExecution();
		}

	}
}

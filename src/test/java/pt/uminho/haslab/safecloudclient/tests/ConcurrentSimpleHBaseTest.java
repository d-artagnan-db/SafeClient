package pt.uminho.haslab.safecloudclient.tests;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.logging.LogFactory;
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
import org.mortbay.log.Log;
import pt.uminho.haslab.safecloudclient.clients.tests.ShareClient;
import pt.uminho.haslab.safecloudclient.clients.tests.TestClient;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.testingutils.ValuesGenerator;

@RunWith(Parameterized.class)
public abstract class ConcurrentSimpleHBaseTest {

    static final org.apache.commons.logging.Log LOG = LogFactory.getLog(ConcurrentSimpleHBaseTest.class.getName());

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
			List<List<BigInteger>> testingIdentifiers) {

		this.testingValues = testingValues;
		this.identifiers = testingIdentifiers;
		testClient = new ShareClient();
		startSignal = new CountDownLatch(nThreads);
	}

	protected void createTestTable() throws ZooKeeperConnectionException,
			IOException, Exception {
		TableName tbname = TableName.valueOf(tableName);
		HTableDescriptor table = new HTableDescriptor(tbname);
		HColumnDescriptor family = new HColumnDescriptor(columnDescriptor);
		table.addFamily(family);
		testClient.createTestTable(table);

	}

	protected abstract class ConcurrentClient extends Thread {
		protected final List<BigInteger> clientIdentifiers;
		protected final byte[] cf;
		protected final byte[] cq;
		protected final HTableInterface table;
		protected boolean testPassed;
		protected final int clientID;

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

		private void fillTable() throws Exception {

			Assert.assertEquals(true, testClient.checkTableExists(tableName));
			for (int i = 0; i < clientIdentifiers.size(); i++) {
				byte[] identifier = clientIdentifiers.get(i).toByteArray();
				byte[] value = testingValues.get(i).toByteArray();
				Put put = new Put(identifier);
				put.add(cf, cq, value);
				table.put(put);

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
				Log.debug(ex);
				throw new IllegalStateException(ex);
			}

		}

	}

	protected abstract Thread getExecutionThread(int i);

	@Test
	public void testBoot(){
        try {
            testClient.startCluster();
            createTestTable();
            assertEquals(true, testClient.checkTableExists(tableName));
            List<Thread> threads = new ArrayList<Thread>();
            
            for (int i = 0; i < nThreads; i++) {
                Thread t = getExecutionThread(i);
                threads.add(t);
            }
            
            for (Thread t : threads) {
                Log.debug("Going to launch client thread");
                t.start();
            }
            
            for (Thread t : threads) {
                Log.debug("Going to join client thread");
                t.join();
            }
            
            for (Thread t : threads) {
                Log.debug("Going to check result of client thread");
                Assert.assertEquals(true, ((ConcurrentClient) t).getTestPassed());
            }
            
            testClient.stopCluster();
        } catch (Exception ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }

	}
}

package pt.uminho.haslab.safecloudclient.tests;

<<<<<<< HEAD
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

=======
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
>>>>>>> Uma rica (not) tarde de testes ...
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.clients.CryptoClient;
import pt.uminho.haslab.safecloudclient.clients.PlaintextClient;
import pt.uminho.haslab.safecloudclient.clients.TestClient;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
import pt.uminho.haslab.safecloudclient.clients.tests.ShareClient;
import pt.uminho.haslab.safecloudclient.clients.tests.TestClient;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.testingutils.ValuesGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public abstract class SimpleHBaseTest {

	static final Log LOG = LogFactory.getLog(SimpleHBaseTest.class.getName());

	protected final String tableName = "TestPutGet";

	protected final String columnDescriptor = "col1";

	@Parameterized.Parameters
	public static Collection valueGenerator() {
		return ValuesGenerator.SingleListValuesGenerator();
	}

	protected final List<BigInteger> testingValues;
	private final List<TestClient> clients;

	protected SimpleHBaseTest(int maxBits, List<BigInteger> values)
			throws Exception {
		testingValues = values;
		clients = addClients();
	}

	public List<TestClient> addClients() throws IOException {
		List<TestClient> clients = new ArrayList<TestClient>();

		System.out.println("Going to create client");

//		clients.add(new PlaintextClient("Vanilla"));
//		clients.add(new CryptoClient("Deterministic", CryptoTechnique.CryptoType.DET));
//		clients.add(new CryptoClient("Standard", CryptoTechnique.CryptoType.STD));
		clients.add(new CryptoClient("OPE", CryptoTechnique.CryptoType.OPE));

		System.out.println("Client created");

		return clients;
	}

	protected void createTestTable(TestClient client)
			throws ZooKeeperConnectionException, IOException, Exception {
		if (!client.checkTableExists(client.getTableName())) {
			TableName tbname = TableName.valueOf(client.getTableName());
			HTableDescriptor table = new HTableDescriptor(tbname);
			HColumnDescriptor family = new HColumnDescriptor(columnDescriptor);
			table.addFamily(family);
			client.createTestTable(table);
		}
	}

	protected void createAndFillTable(TestClient client, HTableInterface table,
			byte[] cf, byte[] cq) throws IOException, InvalidNumberOfBits,
			Exception {
		/*
		 * Test that the creation of the table where the values are going to be
		 * inserted was successful.
		 */
		createTestTable(client);
		Assert.assertEquals(true, client.checkTableExists(tableName));

		BigInteger key = BigInteger.ZERO;
		for (BigInteger value : testingValues) {
			LOG.debug("Going to insert value " + key);
			Put put = new Put(key.toByteArray());

			//Put put = new Put(String.valueOf(key).getBytes());
			put.add(cf, cq, value.toByteArray());
			table.put(put);
			key = key.add(BigInteger.ONE);
		}
	}

	protected abstract void testExecution(TestClient client) throws Exception;

	@Test
	public void testBoot() throws Exception {
		for (TestClient client : clients) {
			client.startCluster();
			createTestTable(client);
			testExecution(client);
			client.stopCluster();

			Configuration conf = new Configuration();
			conf.addResource("conf.xml");

			String table = client.getTableName();

			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(table);
			System.out.println("Table disabled.");
			admin.deleteTable(table);
			System.out.println("Table dropped.");
		}
	}
}

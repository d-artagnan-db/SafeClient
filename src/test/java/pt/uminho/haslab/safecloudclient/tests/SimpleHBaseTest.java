package pt.uminho.haslab.safecloudclient.tests;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import pt.uminho.haslab.safecloudclient.clients.TestClient;

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
import pt.uminho.haslab.safecloudclient.schema.Family;
import pt.uminho.haslab.safecloudclient.schema.SchemaParser;
import pt.uminho.haslab.safecloudclient.schema.TableSchema;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.testingutils.ValuesGenerator;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.clients.CryptoClient;

@RunWith(Parameterized.class)
public abstract class SimpleHBaseTest {

	static final Log LOG = LogFactory.getLog(SimpleHBaseTest.class.getName());

	protected final String columnDescriptor = "col1";

	@Parameterized.Parameters
	public static Collection valueGenerator() {
		return ValuesGenerator.SingleListValuesGenerator();
	}

	protected final List<BigInteger> testingValues;

	private final Map<TestClient, String> clients;

	protected SimpleHBaseTest(int maxBits, List<BigInteger> values)
			throws Exception {
		testingValues = values;
		clients = addClients();
	}

	public Map<TestClient, String> addClients() throws IOException {
		Map<TestClient, String> theClients = new HashMap<TestClient, String>();

		LOG.debug("Creating clients");

		theClients.put(new CryptoClient("s.xml"), "usertable");

		System.out.println("Client created");

		return theClients;
	}

	protected void createTestTable(TestClient client, String tableName)
			throws ZooKeeperConnectionException, IOException, Exception {

		if (!client.checkTableExists(tableName)) {
			ClassLoader cl = getClass().getClassLoader();
			File schemaFile = new File("src/main/resources/s.xml");

			SchemaParser schema = new SchemaParser();
			schema.parse(schemaFile.getPath());
			System.out.println("DATABASE SCHEMA: \n"+schema.printDatabaseSchemas());

			TableSchema ts = schema.getTableSchema("usertable");
//			System.out.println("Table schema: "+ts.toString());

			TableName tbname = TableName.valueOf(ts.getTablename());
			HTableDescriptor table = new HTableDescriptor(tbname);
			for (Family f : ts.getColumnFamilies()) {
				HColumnDescriptor family = new HColumnDescriptor(
						f.getFamilyName());
				table.addFamily(family);
			}
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
		String tableName = clients.get(client);
		createTestTable(client, tableName);
		Assert.assertEquals(true, client.checkTableExists(tableName));

		BigInteger key = BigInteger.ZERO;
		for (BigInteger value : testingValues) {
			LOG.debug("PUT  " + key + " <-> " + value);
			Put put;

			if (!tableName.contains("Share")) {
				put = new Put(Utils.addPadding(key.toByteArray(), 2));
			} else {
				put = new Put(key.toByteArray());
			}

			put.add(cf, cq, value.toByteArray());
			table.put(put);
			key = key.add(BigInteger.ONE);
		}
	}

	protected abstract void testExecution(TestClient client, String tableName)
			throws Exception;

	@Test
	public void testBoot() throws Exception {
		for (TestClient client : clients.keySet()) {
			String tableName = clients.get(client);
			client.startCluster();
			createTestTable(client, tableName);
			testExecution(client, tableName);

			if (!tableName.contains("Share")) {
				Configuration conf = new Configuration();
				conf.addResource("conf.xml");

//				 HBaseAdmin admin = new HBaseAdmin(conf);
//				 admin.disableTable(tableName);
//				 LOG.debug("Table disabled.");
//				 admin.deleteTable(tableName);
//				 LOG.debug("Table dropped.");
			}

			client.stopCluster();

		}
	}
}

package pt.uminho.haslab.safecloudclient.tests;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

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
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.testingutils.ValuesGenerator;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static pt.uminho.haslab.cryptoenv.CryptoTechnique.CryptoType.DET;
import static pt.uminho.haslab.cryptoenv.CryptoTechnique.CryptoType.OPE;
import static pt.uminho.haslab.cryptoenv.CryptoTechnique.CryptoType.STD;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.clients.CryptoClient;
import pt.uminho.haslab.safecloudclient.clients.PlaintextClient;
import pt.uminho.haslab.safecloudclient.clients.ShareClient;

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

                theClients.put(new PlaintextClient(), "Vanilla");
		//theClients.put(new CryptoClient(DET), "Deterministic");
		//theClients.put(new CryptoClient(STD), "Standard");
                theClients.put(new CryptoClient(OPE), "OPE");
                theClients.put(new ShareClient(), "ShareClient");
		System.out.println("Client created");

		return theClients;
	}

	protected void createTestTable(TestClient client, String tableName)
			throws ZooKeeperConnectionException, IOException, Exception {

		if (!client.checkTableExists(tableName)) {
			TableName tbname = TableName.valueOf(tableName);
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
		String tableName = clients.get(client);
		createTestTable(client, tableName);
		Assert.assertEquals(true, client.checkTableExists(tableName));

		BigInteger key = BigInteger.ZERO;
		for (BigInteger value : testingValues) {
			LOG.debug("PUT  " + key  + " <-> " +value);
                        Put put;
                        
                        if(!tableName.contains("Share")){
                            put = new Put(Utils.addPadding(key.toByteArray(), 2));
                        }else{
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

                        
                        if(!tableName.contains("Share")){
                            Configuration conf = new Configuration();
                            conf.addResource("conf.xml");

                            HBaseAdmin admin = new HBaseAdmin(conf);
                            admin.disableTable(tableName);
                            LOG.debug("Table disabled.");
                            admin.deleteTable(tableName);
                            LOG.debug("Table dropped.");
                        }
                        
                        client.stopCluster();

		}
	}
}

package pt.uminho.haslab.safecloudclient.tests;

import pt.uminho.haslab.safecloudclient.clients.tests.TestClient;
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
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.testingutils.ValuesGenerator;
import pt.uminho.haslab.safecloudclient.clients.tests.DefaultHBaseClient;

@RunWith(Parameterized.class)
public abstract class SimpleHBaseTest {

	protected final String tableName = "TestPutGet";

	protected final String columnDescriptor = "values";

	@Parameterized.Parameters
	public static Collection valueGenerator() {
		return ValuesGenerator.SingleListValuesGenerator();
	}

	protected final List<BigInteger> testingValues;
	private final List<TestClient> clients;

	protected SimpleHBaseTest(int maxBits, List<BigInteger> values)
			throws Exception {
		testingValues = values;
		clients = new ArrayList<TestClient>();
		System.out.println("Going to create client");
		// clients.add(new ShareClient());
		clients.add(new DefaultHBaseClient());
		System.out.println("Client created");
	}

	protected void createTestTable(TestClient client)
			throws ZooKeeperConnectionException, IOException, Exception {
		TableName tbname = TableName.valueOf(tableName);
		HTableDescriptor table = new HTableDescriptor(tbname);
		HColumnDescriptor family = new HColumnDescriptor(columnDescriptor);
		table.addFamily(family);
		client.createTestTable(table);

	}

	protected void createAndFillTable(TestClient client, HTableInterface table,
			byte[] cf, byte[] cq) throws IOException, InvalidNumberOfBits,
			Exception {
		/*
		 * Test that the creation of the table where the values are going to be
		 * inserted was successfull.
		 */
		createTestTable(client);
		Assert.assertEquals(true, client.checkTableExists(tableName));

		BigInteger key = BigInteger.ZERO;
		for (BigInteger value : testingValues) {
			System.out.println("Going to insert value " + key);
			Put put = new Put(key.toByteArray());
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
			testExecution(client);
			client.stopCluster();
		}
	}
}

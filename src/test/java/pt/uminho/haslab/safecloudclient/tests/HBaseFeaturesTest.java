package pt.uminho.haslab.safecloudclient.tests;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.clients.TestClient;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by rgmacedo on 2/21/17.
 */
public class HBaseFeaturesTest extends SimpleHBaseTest {

	public Utils utils;

	public HBaseFeaturesTest(int maxBits, List<BigInteger> values)
			throws Exception {
		super(maxBits, values);
		this.utils = new Utils();
	}

	protected void testExecution(TestClient client) throws Exception {
		HTableInterface table = client.createTableInterface(tableName);

		byte[] cf = columnDescriptor.getBytes();
		byte[] cq = "testQualifier".getBytes();

		// createTestTable(client);
		// testPut(table, cf, cq, this.utils.stringToByteArray("1234"));
		// testPut(table, cf, cq, this.utils.stringToByteArray("2000"));
		// testGetRow(table, cf, cq, this.utils.stringToByteArray("1234"));

		createAndFillTable(client, table, cf, cq);
		testScan(table);

	}

	public void testPut(HTableInterface table, byte[] cf, byte[] cq,
			byte[] value) {
		System.out.println("Test Put: ");
		try {
			Put put = new Put(value);
			put.add(cf, cq, "Hello".getBytes());

			table.put(put);

			Get get = new Get(value);
			get.addColumn(cf, cq);
			Result res = table.get(get);
			if (res != null) {
				byte[] storedKey = res.getRow();
				System.out.println("Key " + new String(value)
						+ " inserted successfully: " + res.toString());
			}

		} catch (IOException e) {
			System.out.println("HBaseFeaturesTest: testPut exception. "
					+ e.getMessage());
		}
	}

	public void testGet(HTableInterface table, byte[] cf, byte[] cq) {
		System.out.println("Test Get: \n");
		try {
			BigInteger key = BigInteger.ZERO;
			for (int i = 0; i < testingValues.size(); i++) {
				Get get = new Get(String.valueOf(key).getBytes());
				get.addColumn(cf, cq);
				Result res = table.get(get);
				if (res != null) {
					byte[] storedKey = res.getRow();
					System.out.println("Actual Key: " + String.valueOf(key));
					System.out.println("Stored Key: " + new String(storedKey));
					// assertEquals(key, new BigInteger(storedKey));
				}
				key = key.add(BigInteger.ONE);
			}
		} catch (IOException e) {
			System.out.println("HBaseFeaturesTest: testGet exception. "
					+ e.getMessage());
		}

	}

	public void testGetRow(HTableInterface table, byte[] cf, byte[] cq,
			byte[] value) {
		System.out.println("Test Get: \n");
		try {
			Get get = new Get(value);
			get.addColumn(cf, cq);
			Result res = table.get(get);
			if (res != null) {
				byte[] storedKey = res.getRow();
				System.out.println("Actual Key: " + new String(value));
				System.out.println("Stored Key: " + new String(storedKey));
			}

		} catch (IOException e) {
			System.out.println("HBaseFeaturesTest: testGet exception. "
					+ e.getMessage());
		}
	}

	public void testDelete(HTableInterface table, byte[] cf, byte[] cq) {
		System.out.println("Test Delete:\n");

		BigInteger rem = new BigInteger(this.utils.integerToByteArray(5));
		Delete del = new Delete(rem.toByteArray());
		boolean deleted;
		try {
			table.delete(del);
			Get get = new Get(rem.toByteArray());
			get.addColumn(cf, cq);
			Result res = table.get(get);
			if (res != null) {
				System.out.println("Key " + rem + " not deleted.");
				deleted = false;
			} else {
				System.out.println("Key " + rem + " does not exists.");
				deleted = true;
			}
			assertTrue(deleted);
		} catch (IOException e) {
			System.out.println("HBaseFeaturesTest: testDelete exception. "
					+ e.getMessage());
		}

	}

	public void testScan(HTableInterface table) throws IOException {
		System.out.println("Test Scan:\n");

		Scan s = new Scan();
		// s.setStartRow(String.valueOf(2).getBytes());
		s.setStopRow(String.valueOf(8).getBytes());
		byte[] value = String.valueOf(6).getBytes();

		// System.out.println("Scan Properties: ");
		// System.out.println("Start Row: "+ new String(s.getStartRow()));
		// System.out.println("Stop Row: "+ new String(s.getStopRow()));
		// System.out.println("Filter (<): "+ new String(value));

		Filter filter = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
				new BinaryComparator(value));
		s.setFilter(filter);

		ResultScanner rs = table.getScanner(s);

		for (Result r = rs.next(); r != null; r = rs.next()) {
			if (!r.isEmpty())
				System.out.println("> " + new String(r.getRow()) + " - "
						+ r.toString());
			else
				System.out.println("Is Empty");
		}

	}
}

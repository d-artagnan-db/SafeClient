package pt.uminho.haslab.safecloudclient.tests;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.clients.TestClient;

import java.io.*;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertTrue;

/**
 * Created by rgmacedo on 2/21/17.
 */
public class HBaseFeaturesTest extends SimpleHBaseTest {

	static final Log LOG = LogFactory.getLog(HBaseFeaturesTest.class.getName());

	public Utils utils;

	public HBaseFeaturesTest(int maxBits, List<BigInteger> values)
			throws Exception {
		super(maxBits, values);
		this.utils = new Utils();
	}

	protected void testExecution(TestClient client) {
		HTableInterface table = null;
		try {
			table = client.createTableInterface(client.getTableName());
			System.out.println(client.getTableName());

			long quantity = timingPutTest(table, 60000);
			System.out.println("Quantity: " + quantity);

//			timingScanTest(table, 60000, 100, 50000, 7);
			timingGetTest(table, 60000, quantity);
		} catch (Exception e) {
			System.out.println("Exception in test execution. " + e.getMessage());
		}

	}

	public void testPut(HTableInterface table, byte[] cf, byte[] cq,
			byte[] value) {
		// System.out.println("Test Put: ");
		try {
			Put put = new Put(value);
			put.add(cf, cq, "Hello".getBytes());

			table.put(put);

			// Get get = new Get(value);
			// get.addColumn(cf, cq);
			// Result res = table.get(get);
			// if (res != null) {
			// byte[] storedKey = res.getRow();
			// System.out.println("Key " + new String(value)
			// + " inserted successfully: " + res.toString());
			// }

		} catch (Exception e) {
			System.out.println("HBaseFeaturesTest: testPut exception. "
					+ e.getMessage());
		}
	}

	public void testGet(HTableInterface table, byte[] cf, byte[] cq,
			byte[] value) {
		// System.out.println("Test Get: \n");
		try {
			Get get = new Get(value);
			get.addColumn(cf, cq);
			Result res = table.get(get);
			if (res != null) {
				byte[] storedKey = res.getRow();
				// System.out.println("Actual Key: " + new String(value));
				// System.out.println("Stored Key: " + new String(storedKey));
			}

		} catch (Exception e) {
			System.out.println("HBaseFeaturesTest: testGet exception. "
					+ e.getMessage());
		}
	}

	public void testDelete(HTableInterface table, byte[] cf, byte[] cq,
			byte[] value) {
		System.out.println("Test Delete:\n");

		Delete del = new Delete(value);
		boolean deleted;
		try {
			table.delete(del);
			Get get = new Get(value);
			get.addColumn(cf, cq);
			Result res = table.get(get);
			if (res != null) {
				System.out
						.println("Key " + new String(value) + " not deleted.");
				deleted = false;
			} else {
				System.out.println("Key " + new String(value)
						+ " does not exists.");
				deleted = true;
			}
			assertTrue(deleted);
		} catch (IOException e) {
			System.out.println("HBaseFeaturesTest: testDelete exception. "
					+ e.getMessage());
		}

	}


	public void testScan(HTableInterface table, byte[] startRow, byte[] stopRow)
			throws IOException {
		System.out.println("Test Scan:\n");

		Scan s = new Scan();
		if (startRow != null)
			s.setStartRow(startRow);
		if (startRow != null)
			s.setStopRow(stopRow);

//		long start = System.currentTimeMillis();
		ResultScanner rs = table.getScanner(s);
//		long stop = System.currentTimeMillis();
		int total = 0;
		for (Result r = rs.next(); r != null; r = rs.next()) {
			if (!r.isEmpty()) {
//				System.out.println("> Key:" + new String(r.getRow()));
				total++;
			}
		}

		System.out.println("Total: "+total);

//		System.out.println("Total Scan Time: " + (stop - start) + " ms.");
	}

	public void testFilter(HTableInterface table,
			CompareFilter.CompareOp operation, byte[] compareValue)
			throws IOException {
		System.out.println("Test Filter:\n");
		Scan s = new Scan();

		Filter filter = new RowFilter(operation, new BinaryComparator(
				compareValue));
		s.setFilter(filter);

		ResultScanner rs = table.getScanner(s);

		for (Result r = rs.next(); r != null; r = rs.next()) {
			if (!r.isEmpty())
				System.out.println("> Key: " + new String(r.getRow()));
			else
				System.out.println("No match.");
		}
	}

	public void putGetTest(HTableInterface table, int sizeofVolume) {
		try {
			byte[] cf = columnDescriptor.getBytes();
			byte[] cq = "testQualifier".getBytes();
			List<String> volume = generateVolume(sizeofVolume, 23);

			long start = System.currentTimeMillis();
			for (int i = 0; i < sizeofVolume; i++) {
				Put put = new Put(volume.get(i).getBytes());
				put.add(cf, cq, "Hello".getBytes());
				table.put(put);
			}

			long middle = System.currentTimeMillis();
			for (int i = 0; i < sizeofVolume; i++) {
				Get get = new Get(volume.get(i).getBytes());
				get.addColumn(cf, cq);
				Result res = table.get(get);
				if (res != null) {
					byte[] storedKey = res.getRow();
				}
			}

			long stop = System.currentTimeMillis();

			StringBuilder sb = new StringBuilder();
			sb.append("PutGetTest result:\n");
			sb.append("Total time of execution: ").append((stop - start)).append(" ms\n");
			sb.append("Put execution: ").append((middle - start)).append(" ms\n");
			sb.append("Get execution: ").append((stop - middle)).append(" ms\n");

			LOG.debug(sb.toString());

		} catch (Exception e) {
			LOG.error("Exception in putGetTest. "+e.getMessage());
		}
	}

	public long timingPutTest(HTableInterface table, int time) {
		try {
			byte[] cf = columnDescriptor.getBytes();
			byte[] cq = "testQualifier".getBytes();

			long startTime = System.currentTimeMillis();

			long data = 0;
			while ((System.currentTimeMillis() - startTime) < time) {
				byte[] padded = Utils.addPadding(String.valueOf(data).getBytes(), 7);
				Put put = new Put(padded);
				put.add(cf, cq, "Hello".getBytes());
				table.put(put);

				data++;
			}

			StringBuilder sb = new StringBuilder();
			sb.append("Timing Put Test\n");
			sb.append("Operations: ").append(data).append("\n");
			sb.append("Time: ").append(time).append("\n");
			sb.append("Throughput: ").append((data * 1000) / time).append(" ops/s\n");

			LOG.debug(sb.toString());

			return data;

		} catch(Exception e) {
			LOG.error("Exception in timingPutTest. "+e.getMessage());
		}

		return 0;
	}

	public void timingGetTest(HTableInterface table, int time, long limit) {
		try {
			byte[] cf = columnDescriptor.getBytes();
			byte[] cq = "testQualifier".getBytes();

			long startTime = System.currentTimeMillis();

			long totalOps = 0;
			long data = 0;
			while ((System.currentTimeMillis() - startTime) < time) {
				Get get = new Get(String.valueOf(data).getBytes());
				get.addColumn(cf, cq);
				Result res = table.get(get);
				if (res != null) {
					byte[] storedKey = res.getRow();
				}

				data++;
				totalOps++;
				if (data == limit) {
					data = 0;
				}
			}

			StringBuilder sb = new StringBuilder();
			sb.append("Timing Get Test\n");
			sb.append("Operations: ").append(totalOps).append("\n");
			sb.append("Time: ").append(time).append("\n");
			sb.append("Throughput: ").append(((totalOps * 1000) / time)).append(" ops/s\n");

			LOG.debug(sb.toString());

		} catch(Exception e) {
			LOG.error("Exception in timingGetTest."+e.getMessage());
		}
	}

	public void timingScanTest(HTableInterface table, int time, int startRowBound, int stopRowBound, int formatSize) {
		try {
			Random r = new Random(12345);

			long startTime = System.currentTimeMillis();
			long data = 0;
			while ((System.currentTimeMillis() - startTime) < time) {
				byte[] startRow = Utils.addPadding(String.valueOf(r.nextInt(startRowBound)).getBytes(), formatSize);
				byte[] stopRow = Utils.addPadding(String.valueOf(r.nextInt(stopRowBound)).getBytes(), formatSize);

				Scan s = new Scan();
				s.setStartRow(startRow);
				s.setStopRow(stopRow);

				ResultScanner rs = table.getScanner(s);
				int total = 0;
				for (Result result = rs.next(); result != null; result = rs.next()) {
					if (!result.isEmpty()) {
						total++;
					}
				}
				data++;

				LOG.debug("Scan Result [" + new String(startRow) + ", " + new String(stopRow) + ", " + total + "]");
			}

			StringBuilder sb = new StringBuilder();
			sb.append("TimingScanTest\n");
			sb.append("Operations: ").append(data).append("\n");
			sb.append("Time: ").append(time).append("\n");
			sb.append("Throughput: ").append(((data * 1000) / time)).append(" ops/s\n");

			LOG.debug(sb.toString());

		} catch (Exception e) {
			LOG.error("Exception in timingScanTest. "+e.getMessage());
		}
	}

//	TODO this shouldn't be here (move/replacr in testingUtilities)
	public String generateRandomKey(int size) {
		Random r = new Random();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < size; i++) {
			sb.append(String.valueOf(r.nextInt(9)));

		}
		return sb.toString();
	}

	//	TODO this shouldn't be here (move/replacr in testingUtilities)
	public List<String> generateVolume(int sizeofVolume, int sizeofString) {
		List<String> volume = new ArrayList<String>();
		for (int i = 0; i < sizeofVolume; i++) {
			volume.add(generateRandomKey(sizeofString));
		}
		return volume;
	}

	public static void main(String[] args) {

	}

}

package pt.uminho.haslab.safecloudclient.tests;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.clients.TestClient;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

public class HBaseFeaturesTest extends SimpleHBaseTest {

	static final Log LOG = LogFactory.getLog(HBaseFeaturesTest.class.getName());
	final int formatSize = 23;
	public Utils utils;

	public HBaseFeaturesTest(int maxBits, List<BigInteger> values) throws Exception {
		super(maxBits, values);
		this.utils = new Utils();
	}

	protected void testExecution(TestClient client, String tableName) {
		HTableInterface table;
		int time = 10000;
		try {
			table = client.createTableInterface(tableName);
			LOG.debug("Test Execution [" + tableName + "]\n");

			long quantity = timingPutTest(table, time);
			System.out.println("Quantity: " + quantity);

//			timingGetTest(table, time, quantity);

			// byte[] cf = columnDescriptor.getBytes();
			// byte[] cq = "testQualifier".getBytes();
			// byte[] value = Utils.addPadding("1122330".getBytes(),
			// formatSize);
			//
			// testPut(table, cf, cq, value);
			// testGet(table, cf, cq, value);
			// testDelete(table, cf, cq, value);
//			 testScan(table, null, null);

//			testFilter(table, "RowFilter", CompareFilter.CompareOp.LESS, Utils.addPadding("1500", formatSize));

			testFilter(table, "SingleColumnValueFilter", CompareFilter.CompareOp.LESS, Utils.addPadding("50", formatSize));

			// timingScanTest(table, time, 100, 4000);
			// putGetTest(table, 100);

		} catch (IOException e) {
			LOG.error("Exception in test execution. " + e.getMessage());
		} catch (InvalidNumberOfBits e) {
			LOG.error("Exception in test execution. " + e.getMessage());
		}

	}

	public void testPut(HTableInterface table, byte[] cf, byte[] cq, byte[] value) {
		try {
			long start = System.currentTimeMillis();

			Put put = new Put(value);
			put.add(cf, cq, "Hello".getBytes());

			table.put(put);
			long stop = System.currentTimeMillis();

			Get get = new Get(value);
			get.addColumn(cf, cq);
			Result res = table.get(get);
			if (res != null) {
				byte[] storedKey = res.getRow();
				assertEquals(Arrays.toString(storedKey), Arrays.toString(value));
				LOG.debug("Test Put - Success ["
						+ new String(table.getTableName()) + ","
						+ new String(value) + "," + new String(cf) + ","
						+ new String(cq) + "]\n");
			}

			StringBuilder sb = new StringBuilder();
			sb.append("TestPut\n");
			sb.append("Time: ").append((stop - start)).append("ms\n");

			LOG.debug(sb.toString());

		} catch (IOException e) {
			LOG.error("TestPut exception. " + e.getMessage());
		}
	}

	public void testGet(HTableInterface table, byte[] cf, byte[] cq, byte[] value) {
		try {
			long start = System.currentTimeMillis();

			Get get = new Get(value);
			get.addColumn(cf, cq);
			Result res = table.get(get);
			if (res != null) {
				byte[] storedKey = res.getRow();
				assertEquals(Arrays.toString(storedKey), Arrays.toString(value));
				LOG.debug("Test Get - Success ["
						+ new String(table.getTableName()) + ","
						+ new String(value) + "," + new String(cf) + ","
						+ new String(cq) + "]\n");
			}
			long stop = System.currentTimeMillis();

			StringBuilder sb = new StringBuilder();
			sb.append("TestGet\n");
			sb.append("Time: ").append((stop - start)).append("ms\n");

			LOG.debug(sb.toString());
		} catch (IOException e) {
			LOG.debug("HBaseFeaturesTest: testGet exception. " + e.getMessage());
		}
	}

	public void testDelete(HTableInterface table, byte[] cf, byte[] cq, byte[] value) {
		try {
			long start = System.currentTimeMillis();
			Delete del = new Delete(value);
			boolean deleted;

			table.delete(del);
			Get get = new Get(value);
			get.addColumn(cf, cq);
			Result res = table.get(get);

			if (res != null) {
				if (new String(res.getRow()).equals("")) {
					LOG.debug("Test Delete - Success ["
							+ new String(table.getTableName()) + ","
							+ new String(value) + "," + new String(cf) + ","
							+ new String(cq) + "]\n");
					deleted = true;
				} else {
					LOG.debug("Test Delete - Failed ["
							+ new String(table.getTableName()) + ","
							+ new String(value) + "," + new String(cf) + ","
							+ new String(cq) + "]\n");
					System.out.println(res.toString());
					deleted = false;
				}
			} else {
				LOG.debug("Test Delete - Success ["
						+ new String(table.getTableName()) + ","
						+ new String(value) + "," + new String(cf) + ","
						+ new String(cq) + "]\n");
				deleted = true;
			}
			long stop = System.currentTimeMillis();
			assertTrue(deleted);

			StringBuilder sb = new StringBuilder();
			sb.append("TestDelete\n");
			sb.append("Time: ").append((stop - start)).append("ms\n");

			LOG.debug(sb.toString());

		} catch (IOException e) {
			LOG.error("HBaseFeaturesTest: testDelete exception. "
					+ e.getMessage());
		}

	}

	public void testScan(HTableInterface table, byte[] startRow, byte[] stopRow) {
		try {
			Scan s = new Scan();
			if (startRow != null)
				s.setStartRow(startRow);
			if (startRow != null)
				s.setStopRow(stopRow);

			long start = System.currentTimeMillis();
			ResultScanner rs = table.getScanner(s);

			int total = 0;
			for (Result r = rs.next(); r != null; r = rs.next()) {
				if (!r.isEmpty()) {
					// LOG.debug("Key [" + new String(r.getRow())+"]\n");
					total++;
				}
			}
			long stop = System.currentTimeMillis();

			StringBuilder sb = new StringBuilder();
			sb.append("TestScan\n");

			if (startRow != null)
				sb.append("Start Row: ").append(new String(startRow)).append("\n");
			if (stopRow != null)
				sb.append("Stop Row: ").append(new String(stopRow)).append("\n");

			sb.append("Total Values: ").append(total).append("\n");
			sb.append("Total Scan Time: ").append((stop - start)).append("ms\n");

			LOG.debug(sb.toString());

		} catch (IOException e) {
			LOG.error("Exception in testScan. " + e.getMessage());
		}
	}

	public Filter buildFilter(String filterType, CompareFilter.CompareOp operation, byte[] compareValue) {
		Filter filter = null;

		if(filterType.equals("RowFilter"))
			filter = new RowFilter(operation, new BinaryComparator(compareValue));
		else if(filterType.equals("SingleColumnValueFilter"))
			filter = new SingleColumnValueFilter("Name".getBytes(), "First".getBytes(), operation, new BinaryComparator(compareValue));

		return filter;
	}

	public void testFilter(HTableInterface table, String filterType, CompareFilter.CompareOp operation, byte[] compareValue) {
		try {
			System.out.println("Entrou no testFilter");
			Scan s = new Scan();
			s.setStartRow(Utils.addPadding("1000", formatSize));
			s.setStopRow(Utils.addPadding("1500", formatSize));
			s.setFilter(buildFilter(filterType, operation, compareValue));
			s.addColumn("Name".getBytes(), "First".getBytes());

			System.out.println("Depois de BuildFilter");

			long start = System.currentTimeMillis();
			ResultScanner rs = table.getScanner(s);
			System.out.println("Depois do result scanner");
			int total = 0;
			int decoded = 0;
			for (Result r = rs.next(); r != null; r = rs.next()) {
				if (!r.isEmpty()) {
					System.out.println("Value: "+r.toString());
					System.out.println("Key [" +
							new String(r.getRow())+
							":"+
							new String(r.getValue("Name".getBytes(), "First".getBytes())) +
							"]\n");
					decoded++;
				}
				total++;
			}
			long stop = System.currentTimeMillis();

			StringBuilder sb = new StringBuilder();
			sb.append("TestFilter\n");
			sb.append("Compare Properties: ").append(operation).append(" - ").append(new String(compareValue)).append("\n");
			sb.append("Decoded Values: ").append(decoded).append("\n");
			sb.append("Total Values: ").append(total).append("\n");
			sb.append("Total Filter Time: ").append((stop - start)).append("ms\n");

			System.out.println(sb.toString());

		} catch (IOException e) {
			LOG.error("Exception in testFilter. " + e.getMessage());
		}
	}

	public void putGetTest(HTableInterface table, int sizeofVolume) {
		try {
			byte[] cf = columnDescriptor.getBytes();
			byte[] cq = "testQualifier".getBytes();
			List<String> volume = generateVolume(sizeofVolume, formatSize);

			long start = System.currentTimeMillis();
			for (int i = 0; i < sizeofVolume; i++) {
				Put put = new Put(Utils.addPadding(volume.get(i).getBytes(), formatSize));
				put.add(cf, cq, "Hello".getBytes());
				table.put(put);
			}

			long middle = System.currentTimeMillis();
			for (int i = 0; i < sizeofVolume; i++) {
				Get get = new Get(Utils.addPadding(volume.get(i).getBytes(), formatSize));
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

		} catch (IOException e) {
			LOG.error("Exception in putGetTest. " + e.getMessage());
		}
	}

	public long timingPutTest(HTableInterface table, int time) {
		try {
			byte[] cf = "Name".getBytes();
			byte[] cq = "First".getBytes();
			byte[]cq1 = "Last".getBytes();
			Random r = new Random();

			long startTime = System.currentTimeMillis();

			long data = 0;
			while ((System.currentTimeMillis() - startTime) < time) {
//				byte[] padded = Utils.addPadding(String.valueOf(data).getBytes(), formatSize);
//				byte[] value = Utils.addPadding(String.valueOf(r.nextInt(100000)).getBytes(), formatSize);
				byte[] padded = Utils.addPadding(String.valueOf(data), formatSize);
				byte[] value = Utils.addPadding(String.valueOf(r.nextInt(100)), formatSize);

				Put put = new Put(padded);
				put.add(cf, cq, value);
				put.add(cf, cq1, value);
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

		} catch (IOException e) {
			LOG.error("Exception in timingPutTest. " + e.getMessage());
		}

		return 0;
	}

	public void timingGetTest(HTableInterface table, int time, long limit) {
		try {
			byte[] cf = "Name".getBytes();
			byte[] cq = "First".getBytes();
			byte[]cq1 = "Last".getBytes();

			long startTime = System.currentTimeMillis();

			long totalOps = 0;
			long data = 0;
			while ((System.currentTimeMillis() - startTime) < time) {
				Get get = new Get(Utils.addPadding(String.valueOf(data), formatSize));
				get.addColumn(cf, cq);
				get.addColumn(cf, cq1);
				Result res = table.get(get);
				if (res != null) {
					byte[] storedKey = res.getRow();
					System.out.println("> Key : " + new String(storedKey)+" - "+new String(res.getValue(cf, cq))+" - "+new String(res.getValue(cf, cq1)));
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

		} catch (IOException e) {
			LOG.error("Exception in timingGetTest." + e.getMessage());
		}
	}

	public void timingScanTest(HTableInterface table, int time, int startRowBound, int stopRowBound) {
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

		} catch (IOException e) {
			LOG.error("Exception in timingScanTest. " + e.getMessage());
		}
	}

	// TODO this shouldn't be here (move/replacr in testingUtilities)
	public String generateRandomKey(int size) {
		Random r = new Random();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < size; i++) {
			sb.append(String.valueOf(r.nextInt(9)));

		}
		return sb.toString();
	}

	// TODO this shouldn't be here (move/replacr in testingUtilities)
	public List<String> generateVolume(int sizeofVolume, int sizeofString) {
		List<String> volume = new ArrayList<String>();
		for (int i = 0; i < sizeofVolume; i++) {
			volume.add(generateRandomKey(sizeofString));
		}
		return volume;
	}

}

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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
		HTableInterface table = client.createTableInterface(client.getTableName());
		System.out.println(client.getTableName());

		long quantity = timingPutTest(table, 10000);
		System.out.println("Quantity: "+quantity);

		timingGetTest(table, 10000, quantity);
	}


//		 createTestTable(client);
	//testPut(table, cf, cq, this.utils.stringToByteArray("1234"));
	//testPut(table, cf, cq, this.utils.stringToByteArray("2000"));
//		 testGetRow(table, cf, cq, this.utils.stringToByteArray("1234"));

//		createAndFillTable(client, table, cf, cq);
//		testScan(table);


	public void testPut(HTableInterface table, byte[] cf, byte[] cq, byte[] value) {
		System.out.println("Test Put: ");
		try {
			Put put = new Put(value);
			put.add(cf, cq, "Hello".getBytes());

			table.put(put);

//			Get get = new Get(value);
//			get.addColumn(cf, cq);
//			Result res = table.get(get);
//			if (res != null) {
//				byte[] storedKey = res.getRow();
//				System.out.println("Key " + new String(value)
//						+ " inserted successfully: " + res.toString());
//			}

		} catch (IOException e) {
			System.out.println("HBaseFeaturesTest: testPut exception. "
					+ e.getMessage());
		}
	}

	public void testGet(HTableInterface table, byte[] cf, byte[] cq, byte[] value) {
		System.out.println("Test Get: \n");
		try {
			Get get = new Get(value);
			get.addColumn(cf, cq);
			Result res = table.get(get);
//			if (res != null) {
//				byte[] storedKey = res.getRow();
//				System.out.println("Actual Key: " + new String(value));
//				System.out.println("Stored Key: " + new String(storedKey));
//			}

		} catch (IOException e) {
			System.out.println("HBaseFeaturesTest: testGet exception. " + e.getMessage());
		}
	}

	public void testDelete(HTableInterface table, byte[] cf, byte[] cq, byte[] value) {
		System.out.println("Test Delete:\n");

		Delete del = new Delete(value);
		boolean deleted;
		try {
			table.delete(del);
			Get get = new Get(value);
			get.addColumn(cf, cq);
			Result res = table.get(get);
			if (res != null) {
				System.out.println("Key " + new String(value) + " not deleted.");
				deleted = false;
			} else {
				System.out.println("Key " + new String(value) + " does not exists.");
				deleted = true;
			}
			assertTrue(deleted);
		} catch (IOException e) {
			System.out.println("HBaseFeaturesTest: testDelete exception. "
					+ e.getMessage());
		}

	}


//	TODO fazer um primeiro um pedido à bd (tipo ping) para remover o delay
	public void testScan(HTableInterface table, byte[] startRow, byte[] stopRow) throws IOException {
		System.out.println("Test Scan:\n");

		Scan s = new Scan();
		if(startRow != null)
			s.setStartRow(startRow);
		if(startRow != null)
			s.setStopRow(stopRow);

		long start = System.currentTimeMillis();
		ResultScanner rs = table.getScanner(s);
		long stop = System.currentTimeMillis();

		for (Result r = rs.next(); r != null; r = rs.next()) {
			if (!r.isEmpty())
				System.out.println("> Key:" + new String(r.getRow()));
			else
				System.out.println("No Match.");
		}

		System.out.println("Total Scan Time: "+(stop-start)+" ms.");
	}

	public void testFilter(HTableInterface table, CompareFilter.CompareOp operation, byte[] compareValue) throws IOException {
		System.out.println("Test Filter:\n");
		Scan s = new Scan();

		Filter filter = new RowFilter(operation, new BinaryComparator(compareValue));
		s.setFilter(filter);

		ResultScanner rs = table.getScanner(s);

		for (Result r = rs.next(); r != null; r = rs.next()) {
			if (!r.isEmpty())
				System.out.println("> Key: " + new String(r.getRow()));
			else
				System.out.println("No match.");
		}
	}

	public void putGetTest(HTableInterface table) {
		int sizeofVolume = 1000;
		byte[] cf = columnDescriptor.getBytes();
		byte[] cq = "testQualifier".getBytes();
		List<String> volume =generateVolume(sizeofVolume, 23);

		long start = System.currentTimeMillis();
		for(int i = 0; i < sizeofVolume; i++) {

			testPut(table, cf, cq, volume.get(i).getBytes());
		}
		long middle = System.currentTimeMillis();
		for(int i = 0; i < sizeofVolume; i++) {
			testGet(table, cf, cq, volume.get(i).getBytes());
		}
		long stop = System.currentTimeMillis();

		System.out.println("Total time of execution: "+(stop-start)+" ms.");
		System.out.println("Put execution: "+(middle-start)+" ms.");
		System.out.println("Get execution: "+(stop-middle)+" ms.");

	}

	public long timingPutTest(HTableInterface table, int time) {
		byte[] cf = columnDescriptor.getBytes();
		byte[] cq = "testQualifier".getBytes();

		long startTime = System.currentTimeMillis();

		long data = 0;
		while((System.currentTimeMillis() - startTime) < time) {
			testPut(table, cf, cq, String.valueOf(data).getBytes());
			data++;
		}
		System.out.println("Timing Put Test");
		System.out.println("Operations: "+data);
		System.out.println("Throughput: "+((data*1000)/time)+" ops/s");
		return data;
	}

	public void timingGetTest(HTableInterface table, int time, long limit) {
		byte[] cf = columnDescriptor.getBytes();
		byte[] cq = "testQualifier".getBytes();

		long startTime = System.currentTimeMillis();

		long totalOps = 0;
		long data = 0;
		while((System.currentTimeMillis() - startTime) < time) {
			testGet(table, cf, cq, String.valueOf(data).getBytes());
			data++;
			totalOps++;
			if(data == limit) {
				data = 0;
				System.out.println("RESET.");
			}
		}
		System.out.println("Timing Get Test");
		System.out.println("Operations: "+totalOps);
		System.out.println("Throughput: "+((totalOps*1000)/time)+" ops/s");
	}



	public String generateRandomKey(int size) {
		Random r = new Random();
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < size; i++) {
			sb.append(String.valueOf(r.nextInt(9)));

		}
		return sb.toString();
	}

	public List<String> generateVolume(int sizeofVolume, int sizeofString) {
		List<String> volume = new ArrayList<String>();
		for(int i = 0; i < sizeofVolume; i++) {
			volume.add(generateRandomKey(sizeofString));
		}
		return volume;
	}

}

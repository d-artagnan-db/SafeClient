package pt.uminho.haslab.safecloudclient.tests;

import java.math.BigInteger;
import java.util.List;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import pt.uminho.haslab.safecloudclient.clients.TestClient;

public class WhileMatchFilterTest extends SimpleHBaseTest {

	public WhileMatchFilterTest(int maxBits, List<BigInteger> values)
			throws Exception {
		super(maxBits, values);
	}

	@Override
	protected void testExecution(TestClient client) throws Exception {
		HTableInterface table = client.createTableInterface(tableName);

		byte[] cf = columnDescriptor.getBytes();
		byte[] cq = "testQualifier".getBytes();

		createAndFillTable(client, table, cf, cq);

		// BigInteger valueTest = this.testingValues.get(0);
		BigInteger key = BigInteger.ZERO;
		CompareFilter.CompareOp greater = CompareFilter.CompareOp.GREATER;
		System.out.println("Key to search " + key);
		RowFilter filter = new RowFilter(greater, new BinaryComparator(
				key.toByteArray()));
		WhileMatchFilter mfilter = new WhileMatchFilter(filter);
		Scan theScan = new Scan();
		theScan.setFilter(filter);
		ResultScanner resScanner = table.getScanner(theScan);
		System.out.println("Going to do scan");
		try {
			// Scanners return Result instances.
			// Now, for the actual iteration. One way is to use a while loop
			// like so:
			for (Result rr = resScanner.next(); rr != null; rr = resScanner
					.next()) {
				// print out the row we found and the columns we were looking
				// for
				System.out.println("Found row: " + new BigInteger(rr.getRow()));
			}

			// The other approach is to use a foreach loop. Scanners are
			// iterable!
			// for (Result rr : scanner) {
			// System.out.println("Found row: " + rr);
			// }
		} finally {
			// Make sure you close your scanners when you are done!
			// Thats why we have it inside a try/finally clause
			resScanner.close();
		}

	}
}

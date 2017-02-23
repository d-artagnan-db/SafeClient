package pt.uminho.haslab.safecloudclient.tests;

import pt.uminho.haslab.safecloudclient.clients.tests.TestClient;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.client.Result;
import static org.junit.Assert.assertEquals;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

public class SingleColumnValueFilterTest extends SimpleHBaseTest {

	public SingleColumnValueFilterTest(int maxBits, List<BigInteger> values)
			throws Exception {
		super(maxBits, values);
	}

	@Override
	public void testExecution(TestClient client) throws IOException,
			InvalidNumberOfBits, Exception {

		HTableInterface table = client.createTableInterface(tableName);

		byte[] cf = columnDescriptor.getBytes();
		byte[] cq = "testQualifier".getBytes();

		createAndFillTable(client, table, cf, cq);

		BigInteger valueTest = this.testingValues.get(0);

		BigInteger key = BigInteger.ZERO;
		CompareOp equal = CompareOp.EQUAL;
		SingleColumnValueFilter filter = new SingleColumnValueFilter(cf, cq,
				equal, valueTest.toByteArray());
		Get get = new Get(key.toByteArray());
		get.setFilter(filter);
		Result res = table.get(get);
		assertEquals(true, res.containsColumn(cf, cq));
		assertEquals(true,
				valueTest.equals(new BigInteger(res.getValue(cf, cq))));

	}
}

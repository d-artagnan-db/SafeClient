package pt.uminho.haslab.safecloudclient.tests;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import pt.uminho.haslab.testingutils.ScanValidator;

import static org.junit.Assert.assertEquals;
import pt.uminho.haslab.safecloudclient.clients.TestClient;

public class ScanTest extends SimpleHBaseTest {

	static final Log LOG = LogFactory.getLog(ScanTest.class.getName());

	public ScanTest(int maxBits, List<BigInteger> values) throws Exception {
		super(maxBits, values);
	}

	// Keys are being inserted in an increasing order on the table.
	protected List<BigInteger> genTableKeys() {

		List<BigInteger> keys = new ArrayList<BigInteger>();
		BigInteger val = BigInteger.ZERO;
		for (int i = 0; i < testingValues.size(); i++) {
			keys.add(val);
			val = val.add(BigInteger.ONE);
		}
		return keys;
	}
	@Override
	protected void testExecution(TestClient client, String tableName)
			throws Exception {

		HTableInterface table = client.createTableInterface(tableName);

		byte[] cf = columnDescriptor.getBytes();
		byte[] cq = "testQualifier".getBytes();

		createAndFillTable(client, table, cf, cq);

		ScanValidator shelper = new ScanValidator(genTableKeys());

		byte[] stopKey = shelper.generateStopKey();
		Scan scan = new Scan();
		scan.setStopRow(stopKey);
		ResultScanner scanner = table.getScanner(scan);

		List<Result> results = new ArrayList<Result>();
		for (Result result = scanner.next(); result != null; result = scanner
				.next()) {
			results.add(result);
			LOG.debug("Received key was " + new BigInteger(result.getRow()));
		}

		assertEquals(true, shelper.validateResults(results));

	}

}

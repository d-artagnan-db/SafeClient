package pt.uminho.haslab.safeclient.shareclient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.testingutils.ScanValidator;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
/*
public class LoadBalancerScanTest extends ScanTest {

	static final Log LOG = LogFactory.getLog(LoadBalancerScanTest.class
			.getName());

	public LoadBalancerScanTest(int maxBits, List<BigInteger> values)
			throws Exception {
		super(maxBits, values);
	}

    public TableSchema generateTableSchema() {
        throw new IllegalStateException();
    }

    @Override
	protected void testExecution(TestClient client, String tableName)
			throws Exception {

        HTableInterface table = client.createTableInterface(tableName, generateTableSchema());

		byte[] cf = columnDescriptor.getBytes();
		byte[] cq = "testQualifier".getBytes();

		createAndFillTable(client, table, cf, cq);

		ScanValidator shelper = new ScanValidator(genTableKeys());

		LOG.debug("Testing scan with default Result load balancer");
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

		LOG.debug("Testing scan with static Result load balancer");
		ResultPlayerLoadBalancer lb = new StaticLoadBalancerImpl();
		SharedTable tbl = (SharedTable) table;

		tbl.initializeLoadBalancer(lb);

		scan = new Scan();
		scan.setStopRow(stopKey);
		scanner = table.getScanner(scan);

		results = new ArrayList<Result>();
		for (Result result = scanner.next(); result != null; result = scanner
				.next()) {
			results.add(result);
			LOG.debug("Received key was " + new BigInteger(result.getRow()));
		}

		assertEquals(true, shelper.validateResults(results));

	}
}*/

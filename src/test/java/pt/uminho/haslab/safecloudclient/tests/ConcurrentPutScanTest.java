package pt.uminho.haslab.safecloudclient.tests;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.testingutils.ScanValidator;

public abstract class ConcurrentPutScanTest extends ConcurrentSimpleHBaseTest {

	private final List<BigInteger> allIds;

	public ConcurrentPutScanTest(List<BigInteger> testingValues,
			List<List<BigInteger>> testingIdentifiers) throws Exception {
		super(testingValues, testingIdentifiers);

		allIds = Collections.synchronizedList(new ArrayList<BigInteger>());
		for (List<BigInteger> list : identifiers) {
			for (BigInteger big : list) {
				allIds.add(big);
			}
		}
	}

	protected abstract byte[] getStartKey(ScanValidator validator);

	protected abstract byte[] getStopKey(ScanValidator validator);

	protected class ScanThread extends ConcurrentClient {

		public ScanThread(String resource, int id, String tableName, byte[] cf,
				byte[] cq) throws IOException, InvalidNumberOfBits, Exception {
			super(resource, id, tableName, cf, cq);
		}

		@Override
		protected void testExecution() {
			LOG.debug("Test execution");
			// TODO: increase number of scans
			for (int i = 0; i < 1; i++) {
				try {
					LOG.debug("Going to  start scanValidator");
					ScanValidator shelper = new ScanValidator(allIds);

					byte[] startRow = getStartKey(shelper);
					byte[] stopRow = getStopKey(shelper);
					Scan scan = new Scan();
					if (startRow != null) {
						LOG.debug("Set start row");
						scan.setStartRow(startRow);

					}
					if (stopRow != null) {
						LOG.debug("setting stop row");
						scan.setStopRow(stopRow);
					}
					LOG.debug("Starting scanner");
					ResultScanner scanner = table.getScanner(scan);

					List<Result> receivedResults = new ArrayList<Result>();
					for (Result result = scanner.next(); result != null; result = scanner
							.next()) {
						LOG.debug("Going to do next");
						receivedResults.add(result);
					}
					scanner.close();

					testPassed = shelper.validateResults(receivedResults);

					for (int j = 0; j < clientIdentifiers.size(); j++) {
						BigInteger ident = clientIdentifiers.get(j);
						BigInteger val = testingValues.get(j);

						for (Result r : receivedResults) {
							BigInteger resID = new BigInteger(r.getRow());

							if (resID.equals(ident)) {
								testPassed &= val.equals(new BigInteger(r
										.getValue(cf, cq)));
								LOG.debug("Tests passed " + testPassed);
							}
						}
					}

				} catch (IOException ex) {
					LOG.error(ex);
					throw new IllegalArgumentException(ex);
				}
			}
		}
	}

	@Override
	protected Thread getExecutionThread(int i) {

		String resource = "hbase-client.xml";
		byte[] cf = columnDescriptor.getBytes();
		byte[] cq = "testQualifier".getBytes();

		try {
			LOG.error("Going to get execution thread");
			return new ScanThread(resource, i, tableName, cf, cq);
		} catch (IOException ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} catch (InvalidNumberOfBits ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} catch (Exception ex) {
			LOG.error(ex.getMessage());
			throw new IllegalStateException(ex);
		}

	}

}

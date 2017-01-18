package pt.uminho.haslab.safecloudclient.tests;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

public class ConcurrentPutScanTest extends ConcurrentSimpleHBaseTest {

	public ConcurrentPutScanTest(List<BigInteger> testingValues,
			List<List<BigInteger>> testingIdentifiers) throws Exception {
		super(testingValues, testingIdentifiers);
	}

	private class ScanThread extends ConcurrentClient {
		private final Random rand;
		private final Map<BigInteger, BigInteger> valuesMap;

		public ScanThread(String resource,
				List<BigInteger> clientTestingValues,
				List<BigInteger> clientIdentifiers, String tableName,
				byte[] cf, byte[] cq) throws IOException, InvalidNumberOfBits {
			super(resource, clientTestingValues, clientIdentifiers, tableName,
					cf, cq);
			rand = new Random();
			valuesMap = new HashMap<BigInteger, BigInteger>();

			for (int i = 0; i < clientIdentifiers.size(); i++) {
				valuesMap.put(clientIdentifiers.get(i),
						clientTestingValues.get(i));
			}
		}

		protected void sortValues(List<BigInteger> values) {

			Collections.sort(values, new Comparator<BigInteger>() {
				public int compare(BigInteger value1, BigInteger value2) {
					return value1.compareTo(value2);
				}
			});
		}

		public List<BigInteger> getKeysInRange(List<BigInteger> values,
				BigInteger startRow, BigInteger endRow) {
			List<BigInteger> rangeValues = new ArrayList<BigInteger>();

			for (int i = 0; i < values.size(); i++) {
				BigInteger val = values.get(i);

				boolean greaterOrEqualThan = val.compareTo(startRow) == 0
						|| val.compareTo(startRow) == 1;
				boolean lessOrEqualThan = val.compareTo(endRow) == -1;
				if (greaterOrEqualThan && lessOrEqualThan) {
					rangeValues.add(val);
				}
			}

			return rangeValues;
		}

		public void validateResults(List<BigInteger> localResults,
				List<Result> results) {
			List<BigInteger> receivedResultsBig = new ArrayList<BigInteger>();
			Map<BigInteger, BigInteger> receivedResultsMap = new HashMap<BigInteger, BigInteger>();

			for (Result res : results) {
				receivedResultsBig.add(new BigInteger(res.getRow()));
				byte[] value = res.getValue(cf, cq);
				receivedResultsMap.put(new BigInteger(res.getRow()),
						new BigInteger(value));
			}
			sortValues(receivedResultsBig);
			for (int j = 0; j < receivedResultsBig.size(); j++) {
				BigInteger localRowResult = localResults.get(j);
				BigInteger receivedRowResult = receivedResultsBig.get(j);
				BigInteger localColumnResult = valuesMap.get(localRowResult);
				BigInteger receivedColumnResult = receivedResultsMap
						.get(receivedRowResult);

				testPassed &= localRowResult.equals(receivedRowResult);
				testPassed &= localColumnResult.compareTo(receivedColumnResult) == 0;

			}

		}

		public List<BigInteger> getStartAndStopRow(List<BigInteger> values) {
			List<BigInteger> rows = new ArrayList<BigInteger>();
			BigInteger startRow = values.get(rand.nextInt(values.size()));
			BigInteger stopKey = values.get(rand.nextInt(values.size()));

			BigInteger firstKey = startRow;
			BigInteger secondKey = stopKey;

			if (firstKey.compareTo(secondKey) == 1) {
				startRow = secondKey;
				stopKey = firstKey;
			}
			rows.add(startRow);
			rows.add(stopKey);
			return rows;
		}

		@Override
		protected void testExecution() {
			sortValues(clientIdentifiers);
			for (int i = 0; i < 1; i++) {
				try {

					List<BigInteger> rows = getStartAndStopRow(clientIdentifiers);

					// Scan scan = new Scan(rows.get(0).toByteArray(),
					// rows.get(1)
					// .toByteArray());
					// Scan scan = new Scan(rows.get(0).toByteArray());
					Scan scan = new Scan();
					ResultScanner scanner = table.getScanner(scan);

					List<Result> receivedResults = new ArrayList<Result>();

					for (Result result = scanner.next(); result != null; result = scanner
							.next()) {
						receivedResults.add(result);
					}
					scanner.close();
					System.out.println("Received results are "
							+ receivedResults.size());
					/*
					 * List<BigInteger> localResults = getKeysInRange(
					 * clientIdentifiers, rows.get(0), rows.get(1));
					 * 
					 * validateResults(localResults, receivedResults);
					 */

				} catch (IOException ex) {
					System.out.println(ex.getMessage());
					throw new IllegalArgumentException(ex);
				}
			}
		}
	}

	@Override
	protected Thread getExecutionThread(int i) {
		List<BigInteger> localIdentifiers = identifiers.get(i);
		// System.out.println("Local identifiers "+ localIdentifiers);
		// System.out.println("Local identifiers "+ localIdentifiers.size());
		List<BigInteger> values = testingValues;
		String resource = "hbase-client.xml";
		byte[] cf = columnDescriptor.getBytes();
		byte[] cq = "testQualifier".getBytes();

		try {
			return new ScanThread(resource, values, localIdentifiers,
					tableName, cf, cq);
		} catch (IOException ex) {
			System.out.println(ex.getMessage());
			throw new IllegalStateException(ex);
		} catch (InvalidNumberOfBits ex) {
			System.out.println(ex.getMessage());
			throw new IllegalStateException(ex);
		}

	}

}

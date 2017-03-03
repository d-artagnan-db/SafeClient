package pt.uminho.haslab.safecloudclient.tests;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

public class ConcurrentPutGetTest extends ConcurrentSimpleHBaseTest {

	public ConcurrentPutGetTest(List<BigInteger> testingValues,
			List<List<BigInteger>> testingIdentifiers) throws Exception {
		super(testingValues, testingIdentifiers);
	}

	private class PutThread extends ConcurrentClient {

		public PutThread(String resource, int id, String tableName, byte[] cf,
				byte[] cq) throws IOException, InvalidNumberOfBits, Exception {
			super(resource, id, tableName, cf, cq);
		}

		@Override
		protected void testExecution() {

			for (int i = 0; i < clientIdentifiers.size(); i++) {
				try {
					byte[] identifier = clientIdentifiers.get(i).toByteArray();

					Get get = new Get(identifier);
					Result result = table.get(get);

					testPassed = testingValues.get(i).equals(
							new BigInteger(result.getValue(cf, cq)));
					testPassed &= clientIdentifiers.get(i).equals(
							new BigInteger(result.getRow()));

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
			return new PutThread(resource, i, tableName, cf, cq);
		} catch (IOException ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} catch (InvalidNumberOfBits ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} catch (Exception ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		}
	}

}

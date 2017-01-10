package pt.uminho.haslab.safecloudclient.tests;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Assert;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

public class ConcurrentPutGetTest extends ConcurrentSimpleHBaseTest {

	public ConcurrentPutGetTest(List<BigInteger> testingValues,
			List<List<BigInteger>> testingIdentifiers) throws Exception {
		super(testingValues, testingIdentifiers);
	}

	private class PutThread extends ConcurrentClient {

		public PutThread(String resource, List<BigInteger> clientTestingValues,
				List<BigInteger> clientIdentifiers, String tableName,
				byte[] cf, byte[] cq) throws IOException, InvalidNumberOfBits {
			super(resource, clientTestingValues, clientIdentifiers, tableName,
					cf, cq);
		}

		@Override
		protected void testExecution() {

			System.out.println(Thread.currentThread().getId()
					+ "Going to do get " + clientTestingValues.size());
			for (int i = 0; i < clientIdentifiers.size(); i++) {
				try {
					byte[] identifier = clientIdentifiers.get(i).toByteArray();
					byte[] value = clientTestingValues.get(i).toByteArray();

					Get get = new Get(identifier);
					Result result = table.get(get);
					System.out.println(Thread.currentThread().getId()
							+ "Comparing Values " + new BigInteger(identifier)
							+ " -- " + new BigInteger(result.getValue(cf, cq)));
					Assert.assertArrayEquals(identifier, result.getRow());
					Assert.assertArrayEquals(value, result.getValue(cf, cq));

				} catch (IOException ex) {
					System.out.println(ex);
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
			return new PutThread(resource, values, localIdentifiers, tableName,
					cf, cq);
		} catch (IOException ex) {
			System.out.println(ex);
			throw new IllegalStateException(ex);
		} catch (InvalidNumberOfBits ex) {
			System.out.println(ex);
			throw new IllegalStateException(ex);
		}
	}

}

package pt.uminho.haslab.safecloudclient.tests;

import pt.uminho.haslab.safecloudclient.clients.tests.TestClient;
import org.junit.Test;
import pt.uminho.haslab.cryptoenv.Utils;
//import pt.uminho.haslab.safecloudclient.clients.TestClient;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import static org.junit.Assert.assertEquals;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

public class PutGetTest extends SimpleHBaseTest {

	public PutGetTest(int maxBits, List<BigInteger> values) throws Exception {
		super(maxBits, values);

	}

//	@Override
//	public void testExecution(TestClient client) throws IOException,
//			InvalidNumberOfBits, Exception {
//
//		HTableInterface table = client.createTableInterface(tableName);
//
//		byte[] cf = columnDescriptor.getBytes();
//		byte[] cq = "testQualifier".getBytes();
//
//		createAndFillTable(client, table, cf, cq);
//
//		BigInteger key = BigInteger.ZERO;
//		for (BigInteger value : testingValues) {
//			System.out.println("Going to get value");
//			Get get = new Get(key.toByteArray());
//			get.addColumn(cf, cq);
//			Result res = table.get(get);
//			byte[] storedValue = res.getValue(cf, cq);
//			System.out.println("first val" + value);
//			System.out.println("stored value " + new BigInteger(storedValue));
//			assertEquals(value, new BigInteger(storedValue));
//			key = key.add(BigInteger.ONE);
//
//		}
//
//	}

	@Override
	public void testExecution(TestClient client) throws IOException,
			InvalidNumberOfBits, Exception {

		HTableInterface table = client.createTableInterface(tableName);

		byte[] cf = columnDescriptor.getBytes();
		byte[] cq = "testQualifier".getBytes();

		createAndFillTable(client, table, cf, cq);

		BigInteger key = BigInteger.ZERO;
		for (int i = 0;  i < testingValues.size(); i++) {
			Get get = new Get(key.toByteArray());
			get.addColumn(cf, cq);
			Result res = table.get(get);

			if(!res.isEmpty()) {
                byte[] storedValue = res.getValue(cf, cq);
                System.out.println("Row key is " + new BigInteger(res.getRow()));
                System.out.println("first val" + value);
                System.out.println("stored value " + new BigInteger(storedValue));
                assertEquals(value, new BigInteger(storedValue));
                assertEquals(key, new BigInteger(res.getRow()));
			}
			key = key.add(BigInteger.ONE);
		}
	}

}

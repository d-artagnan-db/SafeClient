package pt.uminho.haslab.safecloudclient.tests;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import static org.junit.Assert.assertEquals;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.clients.TestClient;

public class PutGetTest extends SimpleHBaseTest {

	public PutGetTest(int maxBits, List<BigInteger> values) throws Exception {
		super(maxBits, values);

	}

	@Override
	public void testExecution(TestClient client, String tableName)
			throws IOException, InvalidNumberOfBits, Exception {

		HTableInterface table = client.createTableInterface(tableName);

		byte[] cf = columnDescriptor.getBytes();
		byte[] cq = "testQualifier".getBytes();

		createAndFillTable(client, table, cf, cq);

		BigInteger key = BigInteger.ZERO;
		for (BigInteger value : testingValues) {
			LOG.debug("GET "+ key + " <-> " + value);
                        Get get;
                        
                        
                        if(!tableName.contains("Share")){
                            get = new Get(Utils.addPadding(key.toByteArray(), 23));
                        }else{
                            get = new Get(key.toByteArray());
                        }
                        
			get.addColumn(cf, cq);
			Result res = table.get(get);
			if (!res.isEmpty()) {
				byte[] storedValue = res.getValue(cf, cq);
				LOG.debug("Row key is " + new BigInteger(res.getRow()));
				LOG.debug("first val" + value);
				LOG.debug("stored value " + new BigInteger(storedValue));
				assertEquals(value, new BigInteger(storedValue));
				assertEquals(key, new BigInteger(res.getRow()));
			}
			key = key.add(BigInteger.ONE);

		}

	}

}

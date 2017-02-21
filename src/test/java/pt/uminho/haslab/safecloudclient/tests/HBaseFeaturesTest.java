package pt.uminho.haslab.safecloudclient.tests;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.clients.TestClient;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Created by rgmacedo on 2/21/17.
 */
public class HBaseFeaturesTest extends SimpleHBaseTest {

    public Utils utils;

    public HBaseFeaturesTest(int maxBits, List<BigInteger> values) throws Exception {
        super(maxBits, values);
        this.utils = new Utils();
    }

    protected void testExecution(TestClient client) throws Exception {
        HTableInterface table = client.createTableInterface(tableName);

        byte[] cf = columnDescriptor.getBytes();
        byte[] cq = "testQualifier".getBytes();

        createAndFillTable(client, table, cf, cq);

        testDelete(table, cf, cq);

    }

    public void testPut() {

    }

    public void testGet() {

    }

    public void testDelete(HTableInterface table, byte[] cf, byte[] cq) {
        BigInteger rem = new BigInteger(this.utils.integerToByteArray(5));
        Delete del = new Delete(rem.toByteArray());
        try {
            table.delete(del);
            BigInteger key = BigInteger.ZERO;
            for (int i = 0;  i < testingValues.size(); i++) {
                Get get = new Get(key.toByteArray());
                get.addColumn(cf, cq);
                Result res = table.get(get);
                if(res != null) {
                    byte[] storedKey = res.getRow();
                    System.out.println("Actual Key: " + key);
                    System.out.println("Stored Key: " + new BigInteger(storedKey));
                    assertEquals(key, new BigInteger(storedKey));
                }
                key = key.add(BigInteger.ONE);
        }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void testScan() {

    }
}

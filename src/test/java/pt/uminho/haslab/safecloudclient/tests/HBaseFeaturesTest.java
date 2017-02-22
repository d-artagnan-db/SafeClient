package pt.uminho.haslab.safecloudclient.tests;

import org.apache.hadoop.hbase.client.*;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.clients.TestClient;

import java.io.IOException;
import java.math.BigInteger;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

//        testPut(table, cf, cq);
//        testGet(table, cf, cq);
//        testDelete(table, cf, cq);
        testScan(table);
     }

    public void testPut(HTableInterface table, byte[] cf, byte[] cq) {
        try {
            BigInteger row = new BigInteger(this.utils.integerToByteArray(1234));
            Put put = new Put(row.toByteArray());
            put.add(cf, cq, "Hello".getBytes());

            table.put(put);

            Get get = new Get(row.toByteArray());
            get.addColumn(cf, cq);
            Result res = table.get(get);
            if(res != null) {
                byte[] storedKey = res.getRow();
                System.out.println("Actual Key: " + row);
                System.out.println("Stored Key: " + new BigInteger(storedKey));
                assertEquals(row, new BigInteger(storedKey));
                System.out.println("Key "+row+" inserted successfully: "+res.toString());
            }

        } catch (IOException e) {
            System.out.println("HBaseFeaturesTest: testPut exception. "+e.getMessage());
        }
    }

    public void testGet(HTableInterface table, byte[] cf, byte[] cq) {
        try {
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
            System.out.println("HBaseFeaturesTest: testGet exception. "+e.getMessage());
        }

    }

    public void testDelete(HTableInterface table, byte[] cf, byte[] cq) {
        BigInteger rem = new BigInteger(this.utils.integerToByteArray(5));
        Delete del = new Delete(rem.toByteArray());
        boolean deleted;
        try {
            table.delete(del);
            Get get = new Get(rem.toByteArray());
            get.addColumn(cf, cq);
            Result res = table.get(get);
            if(res != null) {
                System.out.println("Key "+rem+" not deleted.");
                deleted = false;
            }
            else {
                System.out.println("Key "+rem+" does not exists.");
                deleted = true;
            }
            assertTrue(deleted);
        } catch (IOException e) {
            System.out.println("HBaseFeaturesTest: testDelete exception. "+e.getMessage());
        }

    }

    public void testScan(HTableInterface table) throws IOException {
        Scan s = new Scan();
//        s.setStartRow(this.utils.integerToByteArray(5));
//        s.setStopRow(this.utils.integerToByteArray(8));
        ResultScanner rs = table.getScanner(s);

        for(Result r = rs.next(); r != null ; r = rs.next()) {
            if(!r.isEmpty())
                System.out.println("> "+new BigInteger(r.getRow())+" - "+r.toString());
            else
                System.out.println("Is Empty");
        }

    }
}

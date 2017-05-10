package pt.uminho.haslab.safecloudclient.tests;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.junit.Assert;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.clients.TestClient;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;


/**
 * Created by rgmacedo on 5/9/17.
 */
public class FPEHBaseFeaturesTest extends SimpleHBaseTest {
    public Utils utils;
    public Map<Integer,byte[]> sharedMap;

    public FPEHBaseFeaturesTest(int maxBits, List<BigInteger> values) throws Exception {
        super(maxBits, values);
        this.utils = new Utils();
        this.sharedMap = new HashMap<>();
    }

    @Override
    protected void testExecution(TestClient client, String tableName) throws Exception {
        HTableInterface table;
        int time = 10000;
        table = client.createTableInterface(tableName);
        LOG.debug("Test Execution [" + tableName + "]\n");
        System.out.println("Table execution " + tableName);

//        long quantity = timingPutTest(table, time);
//        System.out.println("Quantity: " + quantity);
//        timingGetTest(table, time, quantity);

    }

    public long timingPutTest(HTableInterface table, int time) {
        try {
            byte[] cf = "Physician".getBytes();
            byte[] cq = "Physician ID".getBytes();
            Random r = new Random(100);

            long startTime = System.currentTimeMillis();

            long data = 0;
            int counter = 1234;
            while ((System.currentTimeMillis() - startTime) < time) {
                int cenas = r.nextInt(1000)+100;
                Put put = new Put(Utils.intArrayToByteArray(Utils.integerToIntArray(counter, 10)));
                put.add(cf, cq, Utils.intArrayToByteArray(Utils.integerToIntArray(cenas, 10)));
                table.put(put);
                this.sharedMap.put(counter, put.getRow());
                counter++;
                data++;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("Timing Put Test\n");
            sb.append("Operations: ").append(data).append("\n");
            sb.append("Time: ").append(time).append("\n");
            sb.append("Throughput: ").append((data * 1000) / time).append(" ops/s\n");

            LOG.debug(sb.toString());

            return data;

        } catch (IOException e) {
            LOG.error("Exception in timingPutTest. " + e.getMessage());
        }

        return 0;
    }

    public void timingGetTest(HTableInterface table, int time, long limit) {
        try {
            byte[] cf = "Physician".getBytes();
            byte[] cq = "Physician ID".getBytes();

            long startTime = System.currentTimeMillis();

            long totalOps = 0;
            long data = 0;
            int counter = 1234;
            while ((System.currentTimeMillis() - startTime) < time) {
                Get get = new Get(Utils.intArrayToByteArray(Utils.integerToIntArray(counter, 10)));
                get.addColumn(cf, cq);
                Result res = table.get(get);
                if (res != null || !res.isEmpty()) {
                    byte[] encrypted_value = get.getRow();
//                    byte[] storedKey = res.getRow();
                    if(this.sharedMap.containsKey(counter)) {
                        byte[] plainValue = this.sharedMap.get(counter);
                        boolean value = Arrays.equals(encrypted_value, plainValue);
                        Assert.assertTrue(value);
                        System.out.println("("+counter+","+Arrays.toString(plainValue)+","+Arrays.toString(encrypted_value)+")");
                        totalOps++;
                    }
                    else {
                        System.out.println("Operations: "+totalOps);
                    }
//					System.out.println("> Key : " + new String(storedKey)+" - "+new String(res.getValue(cf, cq)));
                }

                data++;
                counter++;
                if (data == limit) {
                    data = 0;
                }
            }

            StringBuilder sb = new StringBuilder();
            sb.append("Timing Get Test\n");
            sb.append("Operations: ").append(totalOps).append("\n");
            sb.append("Time: ").append(time).append("\n");
            sb.append("Throughput: ").append(((totalOps * 1000) / time)).append(" ops/s\n");

//            LOG.debug(sb.toString());
            System.out.println(sb.toString());
            System.out.println("Shared Map size: "+sharedMap.size()+" - "+totalOps);
        } catch (IOException e) {
            LOG.error("Exception in timingGetTest." + e.getMessage());
        }
    }

}

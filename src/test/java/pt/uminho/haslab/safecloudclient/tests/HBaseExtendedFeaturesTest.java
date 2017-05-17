package pt.uminho.haslab.safecloudclient.tests;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.clients.TestClient;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

public class HBaseExtendedFeaturesTest extends SimpleHBaseTest {

    static final Log LOG = LogFactory.getLog(HBaseFeaturesTest.class.getName());
    final int formatSize = 10;
    public Utils utils;

    public HBaseExtendedFeaturesTest(int maxBits, List<BigInteger> values) throws Exception {
        super(maxBits, values);
        this.utils = new Utils();
    }

    protected void testExecution(TestClient client, String tableName) {
        HTableInterface table;
        try {
            table = client.createTableInterface(tableName);
            LOG.debug("Test Execution [" + tableName + "]\n");
            System.out.println("Table execution "+ tableName);

            testBatchingPuts(table, "Physician".getBytes(), "Physician ID".getBytes(), 10);
            testBatchingGets(table, "Physician".getBytes(), "Physician ID".getBytes(), 5);
            testDelete(table, "Physician".getBytes(), "Physician ID".getBytes());

        } catch (IOException e) {
            LOG.error("Exception in test execution. " + e.getMessage());
        } catch (InvalidNumberOfBits e) {
            LOG.error("Exception in test execution. " + e.getMessage());
        }

    }

//    TODO send results to LOGs
    public void testBatchingPuts(HTableInterface table, byte[] cf, byte[] cq, int batchSize) {
        try {
            List<Put> puts = new ArrayList<>(batchSize);

            for(int i = 0; i < batchSize; i++) {
                Put put = new Put(Utils.addPadding(String.valueOf(i).getBytes(), formatSize));
                put.add(cf, cq, (i+":Hello:"+i).getBytes());
                puts.add(put);
            }

            table.put(puts);

            for(int i = 0; i < batchSize; i++) {
                Get get = new Get(Utils.addPadding(String.valueOf(i).getBytes(), formatSize));
                get.addColumn(cf, cq);
                Result res = table.get(get);
                if (res != null) {
                    byte[] storedKey = res.getRow();
                    assertEquals(Arrays.toString(storedKey), Arrays.toString(Utils.addPadding(String.valueOf(i).getBytes(), formatSize)));
                    System.out.println("Test Put - Success ["
                            + new String(table.getTableName()) + ","
                            + new String(Utils.addPadding(String.valueOf(i).getBytes(), formatSize)) + "," + new String(cf) + ","
                            + new String(cq) + "," + new String(res.getValue(cf, cq)) + "]\n");
                }
            }

        } catch (IOException e) {
            LOG.error("TestPut exception. " + e.getMessage());
        }
    }

//    TODO send result to LOGs
    public void testBatchingGets(HTableInterface table, byte[] cf, byte[] cq, int batchSize) {
        try {
            List<Get> gets = new ArrayList<>(batchSize);

            for (int i = 0; i < batchSize; i++) {
                Get get = new Get(Utils.addPadding(String.valueOf(i).getBytes(), formatSize));
                get.addColumn(cf, cq);
                gets.add(get);
            }

            Result[] res = table.get(gets);
            for (int i = 0; i < batchSize; i++) {
                if (res[i] != null) {
                    byte[] storedKey = res[i].getRow();
                    assertEquals(Arrays.toString(storedKey), Arrays.toString(Utils.addPadding(String.valueOf(i).getBytes(), formatSize)));
                    System.out.println("Test Get - Success ["
                            + new String(table.getTableName()) + ","
                            + new String(storedKey) + "," + new String(cf) + ","
                            + new String(cq) + "," + new String(res[i].getValue(cf, cq)) + "]\n");
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    TODO test
    public void testDelete(HTableInterface table, byte[] cf, byte[] cq) {
        try {
            Delete del = new Delete(Utils.addPadding(String.valueOf(0).getBytes(), formatSize));
//            del.deleteFamily(cf);
            del.deleteColumns(cf,cq);

            table.delete(del);

        } catch (IOException e) {
            LOG.error("HBaseFeaturesTest: testDelete exception. "
                    + e.getMessage());
        }

    }

    public void testScan(HTableInterface table, byte[] startRow, byte[] stopRow) {
        try {
            Scan s = new Scan();
            if (startRow != null)
                s.setStartRow(startRow);
            if (startRow != null)
                s.setStopRow(stopRow);

            long start = System.currentTimeMillis();
            ResultScanner rs = table.getScanner(s);

            int total = 0;
            for (Result r = rs.next(); r != null; r = rs.next()) {
                if (!r.isEmpty()) {
                    // LOG.debug("Key [" + new String(r.getRow())+"]\n");
                    System.out.println("Key [" + Utils.intArrayToInteger(Utils.byteArrayToIntArray(r.getRow()), 10)+"]");
                    total++;
                }
            }
            long stop = System.currentTimeMillis();

            StringBuilder sb = new StringBuilder();
            sb.append("TestScan\n");

            if (startRow != null)
                sb.append("Start Row: ").append(new String(startRow)).append("\n");
            if (stopRow != null)
                sb.append("Stop Row: ").append(new String(stopRow)).append("\n");

            sb.append("Total Values: ").append(total).append("\n");
            sb.append("Total Scan Time: ").append((stop - start)).append("ms\n");

            LOG.debug(sb.toString());

        } catch (IOException e) {
            LOG.error("Exception in testScan. " + e.getMessage());
        }
    }

    public Filter buildFilter(String filterType, CompareFilter.CompareOp operation, byte[] compareValue) {
        Filter filter = null;

        if(filterType.equals("RowFilter"))
            filter = new RowFilter(operation, new BinaryComparator(compareValue));
        else if(filterType.equals("SingleColumnValueFilter"))
            filter = new SingleColumnValueFilter("Physician".getBytes(), "Physician ID".getBytes(), operation, new BinaryComparator(compareValue));

        return filter;
    }

    public void testFilter(HTableInterface table, String filterType, CompareFilter.CompareOp operation, byte[] compareValue) {
        try {
            System.out.println("Entrou no testFilter");
            Scan s = new Scan();
//			s.setStartRow(Utils.addPadding("1000", formatSize));
//			s.setStopRow(Utils.addPadding("1500", formatSize));
            s.setStartRow( Utils.intArrayToByteArray(Utils.integerToIntArray(1234, 10)));
            s.setStopRow( Utils.intArrayToByteArray(Utils.integerToIntArray(4000, 10)));
            s.setFilter(buildFilter(filterType, operation, compareValue));
            s.addColumn("Physician".getBytes(), "Physician ID".getBytes());

            System.out.println("Depois de BuildFilter");

            long start = System.currentTimeMillis();
            ResultScanner rs = table.getScanner(s);
            System.out.println("Depois do result scanner");
            int total = 0;
            int decoded = 0;
            for (Result r = rs.next(); r != null; r = rs.next()) {
                if (!r.isEmpty()) {
                    System.out.println("Value: "+r.toString());
                    System.out.println("Key [" +
//							new String(r.getRow())+
                            Utils.intArrayToInteger(Utils.byteArrayToIntArray(r.getRow()), 10)+
                            ":"+
                            Utils.intArrayToInteger(Utils.byteArrayToIntArray(r.getValue("Physician".getBytes(), "Physician ID".getBytes())), 10) +
                            "]\n");
                    decoded++;
                }
                total++;
            }
            long stop = System.currentTimeMillis();

            StringBuilder sb = new StringBuilder();
            sb.append("TestFilter\n");
            sb.append("Compare Properties: ").append(operation).append(" - ").append(new String(compareValue)).append("\n");
            sb.append("Decoded Values: ").append(decoded).append("\n");
            sb.append("Total Values: ").append(total).append("\n");
            sb.append("Total Filter Time: ").append((stop - start)).append("ms\n");

            System.out.println(sb.toString());

        } catch (IOException e) {
            LOG.error("Exception in testFilter. " + e.getMessage());
        }
    }

    // TODO this shouldn't be here (move/replacr in testingUtilities)
    public String generateRandomKey(int size) {
        Random r = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append(String.valueOf(r.nextInt(9)));

        }
        return sb.toString();
    }

    // TODO this shouldn't be here (move/replacr in testingUtilities)
    public List<String> generateVolume(int sizeofVolume, int sizeofString) {
        List<String> volume = new ArrayList<String>();
        for (int i = 0; i < sizeofVolume; i++) {
            volume.add(generateRandomKey(sizeofString));
        }
        return volume;
    }

}

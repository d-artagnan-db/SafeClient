package pt.uminho.haslab.safecloudclient.tests;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.clients.TestClient;

import java.math.BigInteger;
import java.util.*;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

public class HBaseExtendedFeaturesTest extends SimpleHBaseTest {

    static final Log LOG = LogFactory.getLog(HBaseFeaturesTest.class.getName());
    final int formatSize = 10;
    public Utils utils;
    public boolean padding = false;

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
            testBatchingGets(table, "Physician".getBytes(), "Physician ID".getBytes(), 20);
            testDelete(table, "Physician".getBytes(), "Physician ID".getBytes());
            testBatchingDeletes(table, "Physician".getBytes(), "Physician ID".getBytes(), 5);
            testBatchingPuts(table, "Physician".getBytes(), "Physician ID".getBytes(), 10);

            testGet(table, "Physician".getBytes(), "Physician ID".getBytes(), String.valueOf(2).getBytes());
            testCheckAndPut(table, "Physician".getBytes(), "Physician ID".getBytes(), "2:Hello:2".getBytes());

//            testIncrementColumnValue(table, String.valueOf(2).getBytes(), "Physician".getBytes(), "Incremental".getBytes(), 1L);

            testGetRegionLocation((HTable) table, String.valueOf(2).getBytes());
            testGetRegionLocations((HTable) table);

            testGetRowOrBefore(table, String.valueOf(10).getBytes(), "Physician".getBytes());
            testGetRowOrBefore(table, String.valueOf(0).getBytes(), "Physician".getBytes());

        } catch (IOException e) {
            LOG.error("Exception in test execution. " + e.getMessage());
        } catch (InvalidNumberOfBits e) {
            LOG.error("Exception in test execution. " + e.getMessage());
        }

    }

//    TODO send results to LOGs
    public void testBatchingPuts(HTableInterface table, byte[] cf, byte[] cq, int batchSize) {
        System.out.println("\n== Test put(List<Put> puts) ::");
        try {
            List<Put> puts = new ArrayList<>(batchSize);

            for(int i = 0; i < batchSize; i++) {
                Put put;
                if(padding)
                    put = new Put(Utils.addPadding(String.valueOf(i).getBytes(), formatSize));
                else
                    put = new Put(String.valueOf(i).getBytes());

                put.add(cf, cq, (i+":Hello:"+i).getBytes());
                puts.add(put);
            }

            table.put(puts);

            for(int i = 0; i < batchSize; i++) {
                Get get;
                if(padding)
                    get = new Get(Utils.addPadding(String.valueOf(i).getBytes(), formatSize));
                else
                    get = new Get(String.valueOf(i).getBytes());

                get.addColumn(cf, cq);
                Result res = table.get(get);
                if (res != null) {
                    byte[] storedKey = res.getRow();
                    if(padding)
                        assertEquals(Arrays.toString(storedKey), Arrays.toString(Utils.addPadding(String.valueOf(i).getBytes(), formatSize)));
                    else
                        assertEquals(Arrays.toString(storedKey), Arrays.toString(String.valueOf(i).getBytes()));

                    System.out.println("Test Put - Success ["
                            + new String(table.getTableName()) + ","
                            + new String(String.valueOf(i).getBytes()) + "," + new String(cf) + ","
                            + new String(cq) + "," + new String(res.getValue(cf, cq)) + "]\n");
                }
            }

        } catch (IOException e) {
            LOG.error("TestPut exception. " + e.getMessage());
        }
    }

//    TODO send result to LOGs
    public void testBatchingGets(HTableInterface table, byte[] cf, byte[] cq, int batchSize) {
        System.out.println("\n== Test get(List<Get> gets) ::");
        try {
            List<Get> gets = new ArrayList<>(batchSize);

            for (int i = 0; i < batchSize; i++) {
                Get get;
                if(padding)
                    get = new Get(Utils.addPadding(String.valueOf(i).getBytes(), formatSize));
                else
                    get = new Get(String.valueOf(i).getBytes());

                get.addColumn(cf, cq);
                gets.add(get);
            }

            Result[] res = table.get(gets);
            for (int i = 0; i < batchSize; i++) {
                if (res[i] != null) {
                    byte[] storedKey = res[i].getRow();
                    if(padding)
                        assertEquals(Arrays.toString(storedKey), Arrays.toString(Utils.addPadding(String.valueOf(i).getBytes(), formatSize)));
                    else
                        assertEquals(Arrays.toString(storedKey), Arrays.toString(String.valueOf(i).getBytes()));

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


//    TODO send results to LOGs
    public void testGet(HTableInterface table, byte[] cf, byte[] cq, byte[] row) {
        System.out.println("\n== Test get(byte[] row) ::");
        Get g;
        if(padding)
            g = new Get(Utils.addPadding(row, formatSize));
        else
            g = new Get(row);

        g.addColumn(cf, cq);
        try {
            Result r = table.get(g);
            System.out.println("\nResult: "+r.toString()+"\n");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

//    TODO send results to LOGs
    public void testDelete(HTableInterface table, byte[] cf, byte[] cq) {
        System.out.println("\n==Test delete(byte[] cf, byte[] cq) ::");
        try {
            Delete del;
            if(padding)
                del = new Delete(Utils.addPadding(String.valueOf(0).getBytes(), formatSize));
            else
                del = new Delete(String.valueOf(0).getBytes());

            if(cf != null && cf.length == 0) {
                if(cq == null || cq.length == 0) {
                    del.deleteFamily(cf);
                }
                else {
                    del.deleteColumns(cf, cq);
                }
            }

            table.delete(del);

        } catch (IOException e) {
            LOG.error("HBaseFeaturesTest: testDelete exception. "
                    + e.getMessage());
        }

    }

//    TODO send results to LOGs
    public void testBatchingDeletes(HTableInterface table, byte[] cf, byte[] cq, int batchSize) {
        System.out.println("\n ==Test delete(List<Delete> deletes) ::");
        List<Delete> deletes = new ArrayList<>(batchSize);
        try {
            for(int i = 0; i < batchSize; i++) {
                Delete delete;
                if(padding)
                    delete = new Delete(Utils.addPadding(String.valueOf(i).getBytes(), formatSize));
                else
                    delete = new Delete(String.valueOf(i).getBytes());

                deletes.add(delete);
            }

            table.delete(deletes);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    TODO test for all CryptoTypes (specially STD)
    public void testCheckAndPut(HTableInterface table, byte[] cf, byte[] cq, byte[] value) {
        System.out.println("\n== Test checkAndPut(byte[] cf, byte[] cq, byte[] value) ::");
        Put p;
        if(padding)
            p = new Put(Utils.addPadding(String.valueOf(2).getBytes(),formatSize));
        else
            p = new Put(String.valueOf(2).getBytes());

        p.add(cf, cq, "Hello cenas".getBytes());
        try {
            boolean test;
            if(padding)
                test = table.checkAndPut(Utils.addPadding(String.valueOf(2).getBytes(),formatSize), cf, cq, value, p);
            else
                test = table.checkAndPut(String.valueOf(2).getBytes(), cf, cq, value, p);

            System.out.println("CheckAndPut - "+test);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public void testIncrementColumnValue(HTableInterface table, byte[] row, byte[] cf, byte[] cq, long amount) {
        try {
            System.out.println("\n== Test incrementColumnvalue(byte[] row, byte[] family, byte[] qualifier, long amount) ::");
            if(padding)
                table.incrementColumnValue(Utils.addPadding(row, formatSize), cf, cq, amount);
            else
                table.incrementColumnValue(row, cf, cq, amount);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    TODO send results to LOGs
    public void testGetRegionLocation(HTable table, byte[] row) {
        System.out.println("\n== Test getRegionLocation(byte[] row) ::");
        try {
            HRegionLocation hrl;
            if(padding)
                hrl = table.getRegionLocation(Utils.addPadding(row, formatSize));
            else
                hrl = table.getRegionLocation(row);
            System.out.println("test getRegionLocation : "+hrl.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    TODO send results to LOGs
    public void testGetRegionLocations(HTable table) {
        System.out.println("\n== Test getRegionLocations() ::");
        try {
            NavigableMap<HRegionInfo, ServerName> regions = table.getRegionLocations();
            for(HRegionInfo hri : regions.keySet()) {
                System.out.println("HRegionInfo: "+hri.toString());
                System.out.println("ServerName: "+regions.get(hri).toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    TODO send results to LOGs
    public void testGetRowOrBefore(HTableInterface table, byte[] row, byte[] cf) {
        System.out.println("\n== test getRowOrBefore(byte[] row, byte[] cf) ::");
        try {
            Result res;
            if(padding)
                res = table.getRowOrBefore(Utils.addPadding(row, formatSize), cf);
            else
                res = table.getRowOrBefore(row, cf);

            System.out.println("> "+res.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}

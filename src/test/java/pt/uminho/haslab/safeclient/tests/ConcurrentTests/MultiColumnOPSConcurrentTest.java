package pt.uminho.haslab.safeclient.tests.ConcurrentTests;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import pt.uminho.haslab.safeclient.ExtendedHTable;
import pt.uminho.haslab.safeclient.tests.BaseTests.MultiColumnProtectedTest;
import pt.uminho.haslab.safeclient.tests.Helpers.NOpsClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class MultiColumnOPSConcurrentTest extends MultiColumnProtectedTest {


    //FirstPut is issued by the AbstractTableGenerator for the protected table
    boolean firstPut = true;

    public Scan generateScan() {
        Random r = new Random();
        int index = r.nextInt(rowIdentifiers.size());

        //DET
        byte[] valueAge1 = generatedValues.get("Teste").get("Age1").get(index);
        //OPE
        byte[] valueAge2 = generatedValues.get("Teste").get("Age2").get(index);
        //SMPC
        byte[] valueAge3 = generatedValues.get("Teste").get("Age4").get(index);

        Scan s = new Scan();
        SingleColumnValueFilter scvfAGE1 = new SingleColumnValueFilter("Teste".getBytes(), "Age2".getBytes(), CompareFilter.CompareOp.EQUAL, valueAge1);
        SingleColumnValueFilter scvfAGE2 = new SingleColumnValueFilter("Teste".getBytes(), "Age3".getBytes(), CompareFilter.CompareOp.EQUAL, valueAge2);
        SingleColumnValueFilter scvfAGE4 = new SingleColumnValueFilter("Teste".getBytes(), "Age4".getBytes(), CompareFilter.CompareOp.EQUAL, valueAge3);
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        fl.addFilter(scvfAGE1);
        fl.addFilter(scvfAGE2);
        fl.addFilter(scvfAGE4);
        WhileMatchFilter wmf = new WhileMatchFilter(fl);
        s.setFilter(wmf);
        return s;
    }

    /**
     * Concurrent tests have to create multiple table instances as the default HTable implementation does not support
     * concurrent requests.
     */
    @Override
    protected void put(ExtendedHTable table, List<Put> puts) throws Exception {

        if (puts.size() < getNumberOfThreads()) {
            throw new IllegalStateException("The number of threads has to be lesser than the number of puts");
        }

        int nsplits = puts.size() / getNumberOfThreads();

        List<Thread> clients = new ArrayList<Thread>();
        int index = 0;

        for (int i = 0; i < nsplits; i++) {
            List<Put> clientPuts = puts.subList(index, index + getNumberOfThreads());
            List<Get> gets = new ArrayList<>();

            for (Put p : clientPuts) {
                gets.add(new Get(p.getRow()));
            }

            Scan s = generateScan();
            index = index + getNumberOfThreads();
            ExtendedHTable targetTable;

            if (firstPut) {
                targetTable = getNewProtectedTableInstance();
            } else {
                targetTable = getNewVanillaTableInstance();
            }


            clients.add(new NOpsClient(targetTable, clientPuts, s, gets, i % 3));
        }

        for (Thread t : clients) {
            t.start();
        }
        for (Thread t : clients) {
            t.join();
        }

        firstPut = false;
    }

    protected int getNumberOfThreads() {
        return 20;
    }
}

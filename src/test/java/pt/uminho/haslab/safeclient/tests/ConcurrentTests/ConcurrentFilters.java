package pt.uminho.haslab.safeclient.tests.ConcurrentTests;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.hbaseInterfaces.ExtendedHTable;
import pt.uminho.haslab.safeclient.tests.BaseTests.SingleColumnProtectedTest;
import pt.uminho.haslab.safeclient.tests.Helpers.ScanClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public abstract class ConcurrentFilters extends SingleColumnProtectedTest {


    public Filter generateFilter() {
        Random r = new Random();
        int index = r.nextInt(rowIdentifiers.size());

        //DET
        byte[] valueAge = generatedValues.get("Teste").get("Age").get(index);

        Scan s = new Scan();
        SingleColumnValueFilter scvfAGE = new SingleColumnValueFilter("Teste".getBytes(), "Age".getBytes(), CompareFilter.CompareOp.EQUAL, valueAge);
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        fl.addFilter(scvfAGE);
        s.setFilter(fl);
        return new WhileMatchFilter(fl);
    }

    @Override
    protected void additionalTestExecution(HTableDescriptor vanillaTableDescriptor, HTableDescriptor protectedTableDescriptor) throws Exception {


        List<Thread> vanillaClients = new ArrayList<Thread>();
        List<Thread> protectedClients = new ArrayList<Thread>();
        Scan s = new Scan();
        Filter f = generateFilter();
        s.setFilter(f);


        for (int i = 0; i < getNumberOfThreads(); i++) {
            ExtendedHTable vanillaTable = getNewVanillaTableInstance();
            ExtendedHTable protectedTable = getNewProtectedTableInstance();
            vanillaClients.add(new ScanClient(vanillaTable, s));
            protectedClients.add(new ScanClient(protectedTable, s));
        }


        for (Thread t : vanillaClients) {
            t.start();
        }

        for (Thread t : protectedClients) {
            t.start();
        }

        for (Thread t : vanillaClients) {
            t.join();
        }

        for (Thread t : protectedClients) {
            t.join();
        }

        for (int i = 0; i < getNumberOfThreads(); i++) {
            List<Result> vanillaResults = ((ScanClient) vanillaClients.get(i)).getResults();
            List<Result> protectedResults = ((ScanClient) protectedClients.get(i)).getResults();
            assertEquals(vanillaResults.size(), protectedResults.size());
            for (int j = 0; j < vanillaResults.size(); j++) {
                compareResult(vanillaResults.get(j), protectedResults.get(j));
            }
        }
    }

    protected int getNumberOfThreads() {
        return 50;
    }
}

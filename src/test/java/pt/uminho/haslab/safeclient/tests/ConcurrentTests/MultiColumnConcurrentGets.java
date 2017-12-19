package pt.uminho.haslab.safeclient.tests.ConcurrentTests;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import pt.uminho.haslab.hbaseInterfaces.ExtendedHTable;
import pt.uminho.haslab.safeclient.tests.BaseTests.MultiColumnProtectedTest;
import pt.uminho.haslab.safeclient.tests.Helpers.GetClient;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MultiColumnConcurrentGets extends MultiColumnProtectedTest {
    @Override
    protected void additionalTestExecution(HTableDescriptor vanillaTableDescriptor, HTableDescriptor protectedTableDescriptor) throws Exception {


        List<Thread> vanillaClients = new ArrayList<Thread>();
        List<Thread> protectedClients = new ArrayList<Thread>();

        List<Get> gets = new ArrayList<Get>();


        for (byte[] rowID : rowIdentifiers) {
            Get get = new Get(rowID);
            gets.add(get);
        }


        for (int i = 0; i < getNumberOfThreads(); i++) {
            ExtendedHTable vanillaTable = getNewVanillaTableInstance();
            ExtendedHTable protectedTable = getNewProtectedTableInstance();
            vanillaClients.add(new GetClient(vanillaTable, gets));
            protectedClients.add(new GetClient(protectedTable, gets));
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
            List<Result> vanillaResults = ((GetClient) vanillaClients.get(i)).getResults();
            List<Result> protectedResults = ((GetClient) protectedClients.get(i)).getResults();
            assertEquals(vanillaResults.size(), protectedResults.size());
            for (int j = 0; j < vanillaResults.size(); j++) {
                compareResult(vanillaResults.get(j), protectedResults.get(j));
            }


        }
    }

    protected int getNumberOfThreads() {
        return 20;
    }

}

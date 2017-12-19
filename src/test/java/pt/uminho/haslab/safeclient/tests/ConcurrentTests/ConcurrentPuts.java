package pt.uminho.haslab.safeclient.tests.ConcurrentTests;

import org.apache.hadoop.hbase.client.Put;
import pt.uminho.haslab.hbaseInterfaces.ExtendedHTable;
import pt.uminho.haslab.safeclient.tests.BaseTests.SingleColumnProtectedTest;
import pt.uminho.haslab.safeclient.tests.Helpers.PutClient;

import java.util.ArrayList;
import java.util.List;

public abstract class ConcurrentPuts extends SingleColumnProtectedTest {

    //FirstPut is issued by the AbstractTableGenerator for the protected table
    boolean firstPut = true;

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
            index = index + getNumberOfThreads();
            ExtendedHTable targetTable;

            if (firstPut) {
                targetTable = getNewProtectedTableInstance();
            } else {
                targetTable = getNewVanillaTableInstance();
            }


            clients.add(new PutClient(targetTable, clientPuts));
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

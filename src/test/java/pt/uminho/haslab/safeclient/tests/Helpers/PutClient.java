package pt.uminho.haslab.safeclient.tests.Helpers;

import org.apache.hadoop.hbase.client.Put;
import pt.uminho.haslab.safeclient.ExtendedHTable;

import java.io.IOException;
import java.util.List;

public class PutClient extends Thread {

    private ExtendedHTable table;
    private List<Put> puts;

    public PutClient(ExtendedHTable table, List<Put> puts) {
        this.table = table;
        this.puts = puts;
    }


    @Override
    public void run() {
        for (Put p : puts) {
            try {
                table.put(p);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

}

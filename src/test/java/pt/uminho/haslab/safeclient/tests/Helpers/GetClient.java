package pt.uminho.haslab.safeclient.tests.Helpers;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import pt.uminho.haslab.hbaseInterfaces.ExtendedHTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetClient extends Thread {
    private ExtendedHTable table;
    private List<Get> gets;
    private List<Result> results;


    public GetClient(ExtendedHTable table, List<Get> gets) {
        this.table = table;
        this.gets = gets;
        results = new ArrayList<Result>();
    }


    @Override
    public void run() {
        for (Get g : gets) {
            try {
                results.add(table.get(g));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public List<Result> getResults() {
        return this.results;
    }


}

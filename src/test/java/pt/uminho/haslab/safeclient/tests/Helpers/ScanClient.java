package pt.uminho.haslab.safeclient.tests.Helpers;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import pt.uminho.haslab.safeclient.ExtendedHTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ScanClient extends Thread {

    private ExtendedHTable table;
    private Scan scan;
    private List<Result> results;


    public ScanClient(ExtendedHTable table, Scan scan) {
        this.table = table;
        this.scan = scan;
        results = new ArrayList<Result>();
    }


    @Override
    public void run() {
        try {
            ResultScanner rs = table.getScanner(scan);
            Result res = rs.next();

            while (res != null) {
                results.add(res);
                res = rs.next();
            }

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    public List<Result> getResults() {
        return this.results;
    }

}

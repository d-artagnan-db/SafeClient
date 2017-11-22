package pt.uminho.haslab.safeclient.tests.Helpers;

import org.apache.hadoop.hbase.client.*;
import pt.uminho.haslab.safeclient.ExtendedHTable;

import java.io.IOException;
import java.util.List;

public class NOpsClient extends Thread {

    private final int op;
    private ExtendedHTable table;
    private List<Put> puts;
    private Scan scan;
    private List<Get> gets;


    public NOpsClient(ExtendedHTable table, List<Put> puts, Scan scan, List<Get> get, int op) {
        this.puts = puts;
        this.scan = scan;
        this.gets = get;
        this.op = op;
        this.table = table;
    }

    private void doPuts() {
        for (Put p : puts) {
            try {
                table.put(p);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private void doGet() {
        for (Get g : gets) {
            try {
                table.get(g);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    private void doScan() {
        try {
            ResultScanner rs = table.getScanner(scan);
            Result res = rs.next();

            while (res != null) {
                res = rs.next();
            }

        } catch (IOException e) {
            throw new IllegalStateException(e);
        }

    }

    public void run() {

        if (op == 0) {
            doPuts();
            doGet();
            doScan();
        } else if (op == 1) {
            doGet();
            doPuts();
            doScan();
        } else {
            doScan();
            doGet();
            doPuts();
        }

    }

}

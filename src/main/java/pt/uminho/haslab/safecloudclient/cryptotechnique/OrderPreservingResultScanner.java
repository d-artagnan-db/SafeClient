package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by rgmacedo on 2/21/17.
 */
public class OrderPreservingResultScanner implements ResultScanner {
    public Result next() throws IOException {
        return null;
    }

    public Result[] next(int i) throws IOException {
        return new Result[0];
    }

    public void close() {

    }

    public Iterator<Result> iterator() {
        return null;
    }
}

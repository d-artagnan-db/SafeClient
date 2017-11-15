package pt.uminho.haslab.safeclient.tests.BaseTests;

import org.apache.hadoop.hbase.client.Scan;

import java.util.Random;

public class ScanWithStopKeyTest extends MultiColumnProtectedTest {

    @Override
    protected Scan getTestScan() {
        Random r = new Random();
        int index = r.nextInt(rowIdentifiers.size());
        Scan s = new Scan();
        s.setStopRow(rowIdentifiers.get(index));
        return s;
    }
}

package pt.uminho.haslab.safeclient.tests.SingleColumnValueFilter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import pt.uminho.haslab.safeclient.tests.BaseTests.SimpleDETTest;

import java.util.Random;

public class SingleColumnDETEqualTest extends SimpleDETTest{

    @Override
    protected Scan getTestScan() {
        Random r = new Random();
        int index = r.nextInt(rowIdentifiers.size());

        byte[] value = generatedValues.get("Teste").get("Age").get(index);
        Scan s = new Scan();
        SingleColumnValueFilter scvf = new SingleColumnValueFilter("Teste".getBytes(), "Age".getBytes(), CompareFilter.CompareOp.EQUAL, value);
        s.setFilter(scvf);
        return s;
    }
}

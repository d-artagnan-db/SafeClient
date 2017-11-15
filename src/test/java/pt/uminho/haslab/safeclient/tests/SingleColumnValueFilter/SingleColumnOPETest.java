package pt.uminho.haslab.safeclient.tests.SingleColumnValueFilter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import pt.uminho.haslab.safeclient.tests.BaseTests.SimpleOPETest;

import java.util.Random;

public abstract class SingleColumnOPETest extends SimpleOPETest{


    protected abstract CompareFilter.CompareOp getOperation();

    @Override
    protected Scan getTestScan() {
        Random r = new Random();
        int index = r.nextInt(rowIdentifiers.size());

        byte[] value = generatedValues.get("Teste").get("Age").get(index);
        Scan s = new Scan();
        SingleColumnValueFilter scvf = new SingleColumnValueFilter("Teste".getBytes(), "Age".getBytes(), getOperation(), value);
        s.setFilter(scvf);
        return s;
    }

}

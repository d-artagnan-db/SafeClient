package pt.uminho.haslab.safeclient.tests.SingleColumnValueFilter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import pt.uminho.haslab.safeclient.tests.BaseTests.SimpleDETTest;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

//This test should make cryptoclient throw two types of exceptions, UnsupportedOperationException and IllegalStateException
public class SingleColumnDETGTETest extends SimpleDETTest {

    @Override
    protected Scan getTestScan() {
        Random r = new Random();
        int index = r.nextInt(rowIdentifiers.size());

        byte[] value = generatedValues.get("Teste").get("Age").get(index);
        Scan s = new Scan();
        SingleColumnValueFilter scvf = new SingleColumnValueFilter("Teste".getBytes(), "Age".getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, value);
        s.setFilter(scvf);
        return s;
    }

    protected boolean expectException() {
        return true;
    }

    protected Set<String> getExpectedExceptionNames() {
        Set<String> expectedExceptions = new HashSet<String>();
        expectedExceptions.add(UnsupportedOperationException.class.getName());
        expectedExceptions.add(IllegalStateException.class.getName());
        return expectedExceptions;

    }

}

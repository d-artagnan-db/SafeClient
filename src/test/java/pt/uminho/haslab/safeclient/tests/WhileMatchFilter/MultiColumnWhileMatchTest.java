package pt.uminho.haslab.safeclient.tests.WhileMatchFilter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import pt.uminho.haslab.safeclient.tests.BaseTests.MultiColumnProtectedTest;

import java.util.Random;

public class MultiColumnWhileMatchTest extends MultiColumnProtectedTest {


    @Override
    protected Scan getTestScan() {
        Random r = new Random();
        int index = r.nextInt(rowIdentifiers.size());

        //DET
        byte[] valueAge2 = generatedValues.get("Teste").get("Age2").get(index);
        //OPE
        byte[] valueAge3 = generatedValues.get("Teste").get("Age3").get(index);
        //SMPC
        byte[] valueAge4 = generatedValues.get("Teste").get("Age4").get(index);

        Scan s = new Scan();
        SingleColumnValueFilter scvfAge2 = new SingleColumnValueFilter("Teste".getBytes(), "Age2".getBytes(), CompareFilter.CompareOp.EQUAL, valueAge2);
        SingleColumnValueFilter scvfAge3 = new SingleColumnValueFilter("Teste".getBytes(), "Age3".getBytes(), CompareFilter.CompareOp.EQUAL, valueAge3);
        SingleColumnValueFilter scvfAge4 = new SingleColumnValueFilter("Teste".getBytes(), "Age4".getBytes(), CompareFilter.CompareOp.EQUAL, valueAge4);
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        fl.addFilter(scvfAge2);
        fl.addFilter(scvfAge3);
        fl.addFilter(scvfAge4);
        WhileMatchFilter wmf = new WhileMatchFilter(fl);
        s.setFilter(wmf);
        return s;
    }
}

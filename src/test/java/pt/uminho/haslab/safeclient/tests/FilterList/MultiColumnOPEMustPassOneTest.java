package pt.uminho.haslab.safeclient.tests.FilterList;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import pt.uminho.haslab.safeclient.helpers.AdminProxy;
import pt.uminho.haslab.safeclient.helpers.CryptoClient;
import pt.uminho.haslab.safemapper.DatabaseSchema;

import java.util.Random;

public class MultiColumnOPEMustPassOneTest extends MultiColumnCTypeTest {

    @Override
    protected DatabaseSchema.CryptoType getProtectedColumnType() {
        return DatabaseSchema.CryptoType.OPE;
    }

    @Override
    protected AdminProxy getProtectedHBaseAdmin() throws Exception {
        return new CryptoClient("protected-hbase-client.xml");
    }

    @Override
    protected String getProtectedSchemaPath() {
        return null;
    }

    protected int getProtectedColumnFormatSize() {
        return 4;
    }

    @Override
    protected Scan getTestScan() {
        Random r = new Random();
        int index = r.nextInt(rowIdentifiers.size());

        byte[] valueAge1 = generatedValues.get("Teste").get("Age1").get(index);
        byte[] valueAge2 = generatedValues.get("Teste").get("Age2").get(index);
        byte[] valueAge3 = generatedValues.get("Teste").get("Age3").get(index);

        Scan s = new Scan();
        SingleColumnValueFilter scvfAGE1 = new SingleColumnValueFilter("Teste".getBytes(), "Age1".getBytes(), CompareFilter.CompareOp.EQUAL, valueAge1);
        SingleColumnValueFilter scvfAGE2 = new SingleColumnValueFilter("Teste".getBytes(), "Age2".getBytes(), CompareFilter.CompareOp.EQUAL, valueAge2);
        SingleColumnValueFilter scvfAGE3 = new SingleColumnValueFilter("Teste".getBytes(), "Age3".getBytes(), CompareFilter.CompareOp.EQUAL, valueAge3);
        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        fl.addFilter(scvfAGE1);
        fl.addFilter(scvfAGE2);
        fl.addFilter(scvfAGE3);
        s.setFilter(fl);
        return s;
    }
}

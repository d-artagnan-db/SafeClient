package pt.uminho.haslab.safeclient.tests.SingleColumnValueFilter;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class LesssOrEqualThanOPETest extends SingleColumnOPETest {

    @Override
    protected CompareFilter.CompareOp getOperation() {
        return CompareFilter.CompareOp.LESS_OR_EQUAL;
    }

}

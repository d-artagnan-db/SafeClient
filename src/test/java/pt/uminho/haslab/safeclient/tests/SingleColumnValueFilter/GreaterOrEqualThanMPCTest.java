package pt.uminho.haslab.safeclient.tests.SingleColumnValueFilter;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class GreaterOrEqualThanMPCTest extends SingleColumnMPCTest {
    @Override
    protected CompareFilter.CompareOp getOperation() {
        return CompareFilter.CompareOp.GREATER_OR_EQUAL;
    }
}

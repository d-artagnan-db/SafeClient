package pt.uminho.haslab.safeclient.tests.SingleColumnValueFilter;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class GreaterThanMPCTest extends SingleColumnMPCTest {
    @Override
    protected CompareFilter.CompareOp getOperation() {
        return CompareFilter.CompareOp.GREATER;
    }
}

package pt.uminho.haslab.safeclient.tests.SingleColumnValueFilter;

import org.apache.hadoop.hbase.filter.CompareFilter;

public class LessThanMPCTest extends SingleColumnMPCTest{
    @Override
    protected CompareFilter.CompareOp getOperation() {
        return CompareFilter.CompareOp.LESS;
    }
}

package pt.uminho.haslab.safecloudclient.cryptotechnique.resultscanner;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class ResultScannerAbstratClass implements ResultScanner {

    public CryptoProperties cryptoProperties;
    public ResultScanner scanner;
    public byte[] startRow;
    public byte[] stopRow;
    public boolean hasStartRow;
    public boolean hasEndRow;
    public boolean hasFilter;
    public CompareFilter.CompareOp compareOp;
    public byte[] compareValue;
    public String filterType;


    public ResultScannerAbstratClass(CryptoProperties cryptoProperties, ResultScanner encryptedResultScanner) {
        this.cryptoProperties = cryptoProperties;
        this.scanner = encryptedResultScanner;
    }


    public ResultScannerAbstratClass(CryptoProperties cryptoProperties, ResultScanner encryptedResultScanner, byte[] startRow, byte[] stopRow, Object filterResult) {
        this.cryptoProperties = cryptoProperties;
        this.scanner = encryptedResultScanner;
        this.startRow = startRow;
        this.stopRow = stopRow;
        this.setFilters(startRow, stopRow, filterResult);
    }


    public void setFilters(byte[] startRow, byte[] stopRow, Object filter) {
        if (startRow != null && startRow.length > 0) {
            this.hasStartRow = true;
            this.startRow = startRow;
        } else {
            this.hasStartRow = false;
        }

        if (stopRow != null && stopRow.length > 0) {
            this.hasEndRow = true;
            this.stopRow = stopRow;
        } else {
            this.hasEndRow = false;
        }

        if (filter != null) {
            this.hasFilter = true;
            Object[] filterProperties = (Object[]) filter;
            if(filterProperties.length == 2) {
                this.filterType = "RowFilter";
                this.compareOp = (CompareFilter.CompareOp) filterProperties[0];
                this.compareValue = (byte[]) filterProperties[1];
            }
            else if(filterProperties.length == 4) {
                this.filterType = "SingleColumnValueFilter";
                this.compareOp = (CompareFilter.CompareOp) filterProperties[2];
                this.compareValue = (byte[]) filterProperties[3];
            }
        } else {
            this.hasFilter = false;
        }
    }

    public boolean checkRow(byte[] row) {
        boolean digest;
        Bytes.ByteArrayComparator byteArrayComparator = new Bytes.ByteArrayComparator();

        if (hasStartRow && hasEndRow) {
            digest = (byteArrayComparator.compare(row, startRow) >= 0 && byteArrayComparator.compare(row, stopRow) < 0);
        } else if (hasStartRow && !hasEndRow) {
            digest = (byteArrayComparator.compare(row, startRow) >= 0);
        } else if (hasEndRow) {
            digest = (byteArrayComparator.compare(row, stopRow) < 0);
        } else {
            digest = true;
        }

        return digest;
    }

    public boolean checkValue(byte[] value) {
        boolean digest;
        Bytes.ByteArrayComparator byteArrayComparator = new Bytes.ByteArrayComparator();

        switch (this.compareOp) {
            case EQUAL :
                digest = (byteArrayComparator.compare(this.compareValue, value) == 0);
                break;
            default:
                digest = false;
                break;
        }
        return digest;
    }

    @Override
    public abstract Result next() throws IOException;

    @Override
    public abstract Result[] next(int i) throws IOException;

    @Override
    public abstract void close();

    @Override
    public abstract Iterator<Result> iterator();

}

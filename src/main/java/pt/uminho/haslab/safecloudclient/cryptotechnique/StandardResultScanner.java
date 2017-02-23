package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Iterator;

/**
 * Created by rgmacedo on 2/21/17.
 */

public class StandardResultScanner implements ResultScanner {
    public ResultScanner scanner;
    public CryptoProperties cProperties;
    public byte[] startRow;
    public byte[] endRow;
    public boolean hasStartRow;
    public boolean hasEndRow;
    public boolean hasFilter;
    public BigInteger start;
    public BigInteger end;
    public CompareFilter.CompareOp compareOp;
    public byte[] compareValue;

    public StandardResultScanner(CryptoProperties cp, byte[] startRow, byte[] endRow, ResultScanner encryptedScanner, Object filterResult) {
        this.scanner = encryptedScanner;
        this.cProperties = cp;
        this.startRow = startRow;
        this.endRow = endRow;
        this.setFilters(startRow, endRow, filterResult);
    }

    public void setFilters(byte[] startRow, byte[] endRow, Object filter) {
        if(startRow.length != 0) {
            this.hasStartRow = true;
            this.startRow = startRow;
            this.start = new BigInteger(this.startRow);
        }
        else {
            this.hasStartRow = false;
            this.start = new BigInteger(this.cProperties.utils.integerToByteArray(0));
        }

        if(endRow.length != 0) {
            this.hasEndRow = true;
            this.endRow = endRow;
            this.end = new BigInteger(endRow);
        }
        else {
            this.hasEndRow = false;
        }

        if(filter != null) {
            this.hasFilter = true;
            Object[] filterProperties = (Object[]) filter;
            this.compareOp = (CompareFilter.CompareOp) filterProperties[0];
            this.compareValue= (byte[]) filterProperties[1];
        }
        else {
            this.hasFilter = false;
        }
    }

    public boolean digestStartEndRow(BigInteger row) {
        boolean digest;
        if(hasStartRow && hasEndRow) {
            digest = (row.compareTo(start) >= 0 && row.compareTo(end) < 0);
        }
        else if(hasStartRow && !hasEndRow){
            digest = (row.compareTo(start) >= 0);
        }
        else if(hasEndRow) {
            digest = (row.compareTo(end) < 0);
        }
        else {
            digest = true;
        }

        return digest;
    }

    public boolean digestFilter(BigInteger row, BigInteger value) {
        boolean digest = true;
        switch (this.compareOp) {
            case EQUAL:
                digest = (row.compareTo(value) == 0);
                break;
            case GREATER:
                digest = (row.compareTo(value) > 0);
                break;
            case LESS:
                digest = (row.compareTo(value) < 0);
                break;
            case GREATER_OR_EQUAL:
                digest = (row.compareTo(value) >= 0);
                break;
            case LESS_OR_EQUAL:
                digest = (row.compareTo(value) < 0);
                break;
        }
        return digest;
    }


    public Result next() throws IOException {
        Result res = this.scanner.next();
        boolean digest;
        if(res!=null) {
            BigInteger row = new BigInteger(this.cProperties.decode(res.getRow()));

            digest = digestStartEndRow(row);

            if(hasFilter && digest) {
                BigInteger value = new BigInteger(this.compareValue);
                digest = digestFilter(row, value);
            }

            if(digest)
                return this.cProperties.decodeResult(res.getRow(), res);
            else
                return new Result();

        }
        else
            return null;
    }

    public Result[] next(int i) throws IOException {
        return this.scanner.next(i);
    }

    public void close() {
        this.scanner.close();
    }

    public Iterator<Result> iterator() {
        return this.scanner.iterator();
    }
}

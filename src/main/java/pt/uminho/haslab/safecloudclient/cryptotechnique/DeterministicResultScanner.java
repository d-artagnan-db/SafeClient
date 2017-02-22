package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Iterator;

/**
 * Created by rgmacedo on 2/21/17.
 */
public class DeterministicResultScanner implements ResultScanner {
    public CryptoProperties cProperties;
    public ResultScanner encryptedScanner;
    public byte[] startRow;
    public byte[] endRow;
    public boolean hasStartRow;
    public boolean hasEndRow;
    public boolean hasFilter;
    public BigInteger start;
    public BigInteger end;

    public DeterministicResultScanner(CryptoProperties cp, byte[] startRow, byte[] endRow, ResultScanner encryptedScanner) {
        this.cProperties = cp;
        this.encryptedScanner = encryptedScanner;
        this.startRow = startRow;
        this.endRow = endRow;
        this.setFilters(startRow, endRow);
    }

    public void setFilters(byte[] startRow, byte[] endRow) {
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

        this.hasFilter = false;

    }

    public Result next() throws IOException {
        Result res = this.encryptedScanner.next();
        if(res!=null) {
            BigInteger row = new BigInteger(this.cProperties.decode(res.getRow()));

            if(hasStartRow && hasEndRow) {
                if (row.compareTo(start) >= 0 && row.compareTo(end) < 0) {
                    return this.cProperties.decodeResult(res.getRow(), res);
                } else
                    return new Result();
            }
            else if(hasStartRow && !hasEndRow){
                if (row.compareTo(start) >= 0) {
                    return this.cProperties.decodeResult(res.getRow(), res);
                } else
                    return new Result();
            }
            else if(hasEndRow) {
                if (row.compareTo(end) < 0) {
                    return this.cProperties.decodeResult(res.getRow(), res);
                } else
                    return new Result();
            }
            else {
                return this.cProperties.decodeResult(res.getRow(), res);
            }
        }
        else
            return null;

    }

    public Result[] next(int i) throws IOException {
        return encryptedScanner.next(i);
    }

    public void close() {
        this.encryptedScanner.close();
    }

    public Iterator<Result> iterator() {
        return this.encryptedScanner.iterator();
    }
}

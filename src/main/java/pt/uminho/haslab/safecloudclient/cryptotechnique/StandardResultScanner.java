package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.cryptoenv.Utils;

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
    public RowFilter rw;

    public StandardResultScanner(CryptoProperties cp, byte[] startRow, byte[] endRow, ResultScanner encryptedScanner, Filter filter) {
        this.scanner = encryptedScanner;
        this.cProperties = cp;
        this.startRow = startRow;
        this.endRow = endRow;
        this.setFilters(startRow, endRow, (RowFilter) filter);
    }

    public void setFilters(byte[] startRow, byte[] endRow, RowFilter filter) {
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
            this.compareOp = filter.getOperator();
            this.compareValue= filter.getComparator().getValue();
            this.rw = filter;
            System.out.println("Filter: "+this.compareOp+" - "+new BigInteger(compareValue));
        }
        else {
            this.hasFilter = false;
        }
    }


    public Result next() throws IOException {
        Result res = this.scanner.next();
        if(res!=null) {
            Result digestedResult;
            BigInteger row = new BigInteger(this.cProperties.decode(res.getRow()));

            if(hasStartRow && hasEndRow) {
                if (row.compareTo(start) >= 0 && row.compareTo(end) < 0) {
                    digestedResult = this.cProperties.decodeResult(res.getRow(), res);
                } else
                    return new Result();
            }
            else if(hasStartRow && !hasEndRow){
                if (row.compareTo(start) >= 0) {
                    digestedResult = this.cProperties.decodeResult(res.getRow(), res);
                } else
                    return new Result();
            }
            else if(hasEndRow) {
                if (row.compareTo(end) < 0) {
                    digestedResult = this.cProperties.decodeResult(res.getRow(), res);
                } else
                    digestedResult = new Result();
            }
            else {
                digestedResult = this.cProperties.decodeResult(res.getRow(), res);
            }

            if(hasFilter) {
                BigInteger value = new BigInteger(this.compareValue);
//                System.out.println("CompareFilter: "+compareOp+"\nBinaryComparator:"+(rw.getComparator().compareTo(this.cProperties.utils.integerToByteArray(4)))+" - "+value);
//                System.out.println("CompareFilter: "+compareOp+"\nBinaryComparator:"+(rw.getComparator().compareTo(this.cProperties.utils.integerToByteArray(10)))+" - "+value);
//                System.out.println("CompareFilter: "+compareOp+"\nBinaryComparator:"+(rw.getComparator().compareTo(this.cProperties.utils.integerToByteArray(3)))+" - "+value);


                switch (this.compareOp) {
                    case EQUAL:
                        if(row.compareTo(value) == 0)
                            System.out.println(row+" EQUAL to "+value);
                        break;
                    case GREATER:
                        if(row.compareTo(value) > 0)
                            System.out.println(row+" GREATER than "+value);
                        break;
                    case LESS:
                        if(row.compareTo(value) < 0)
                            System.out.println(row+" LESS than "+value);
                        break;
                }
            }

            return digestedResult;
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

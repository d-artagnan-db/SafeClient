package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
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


    public StandardResultScanner(CryptoProperties cp, byte[] startRow, byte[] endRow, ResultScanner encryptedScanner) {
        this.scanner = encryptedScanner;
        this.cProperties = cp;
        setFilters(startRow, endRow);

    }

    public void setFilters(byte[] startRow, byte[] endRow) {
        if(startRow.length != 0) {
            this.hasStartRow = true;
            this.startRow = startRow;
            this.start = new BigInteger(this.cProperties.utils.integerToByteArray(2));
            System.out.println("Start Row: "+this.start);
        }
        else {
            this.hasStartRow = false;
            this.start = new BigInteger(this.cProperties.utils.integerToByteArray(2));
        }

        if(endRow.length != 0) {
            this.hasEndRow = true;
            this.endRow = endRow;
            this.end = new BigInteger(this.cProperties.utils.integerToByteArray(7));
            System.out.println("End Row: "+this.end);
        }
        else {
            this.hasEndRow = false;
            this.end = new BigInteger(this.cProperties.utils.integerToByteArray(7));
        }

        this.hasFilter = false;

    }



    public Result next() throws IOException {
        Result res = this.scanner.next();
        if(res!=null) {
            BigInteger row = new BigInteger(this.cProperties.decode(res.getRow()));
//            if(hasEndRow) {
                if (row.compareTo(this.start) >= 0 && row.compareTo(this.end) < 0) {
                    return this.cProperties.decodeResult(res.getRow(), res);
                } else
                    return new Result();
//                TODO ver isto porque está sempre a enviar, tenha ou nao result
//            }

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

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
    public CryptoTable cryptoTable;
    public byte[] startRow;
    public byte[] endRow;
    public boolean hasRowFilter;
    public Utils utils;
//    public String comparator;
//    public String compareValue;

    public StandardResultScanner(byte[] key, byte[] startRow, byte[] endRow, ResultScanner encScanner) {
        this.scanner = encScanner;
        this.cryptoTable = new CryptoTable(CryptoTechnique.CryptoType.STD);
        this.cryptoTable.key = key;
        this.hasRowFilter = false;
        this.utils = new Utils();
        this.startRow = startRow;
        this.endRow = endRow;
    }

    public Result next() throws IOException {
        Result res = this.scanner.next();
        if(!hasRowFilter && res != null) {
            BigInteger row = new BigInteger(this.cryptoTable.decode(res.getRow()));
//            TODO if's para o caso das start/end rows serem vazias
            BigInteger st = new BigInteger(this.utils.integerToByteArray(0));
            BigInteger e = new BigInteger(this.utils.integerToByteArray(10));
            if(row.compareTo(st) >= 0 && row.compareTo(e) < 0) {
                return this.cryptoTable.decodeResult(row.toByteArray(), res);
            }
        }
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

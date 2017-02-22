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

    public DeterministicResultScanner(CryptoProperties cp, ResultScanner encryptedScanner) {
        this.cProperties = cp;
        this.encryptedScanner = encryptedScanner;
    }

    public Result next() throws IOException {
        Result res = this.encryptedScanner.next();
        if(res!=null) {
            BigInteger row = new BigInteger(this.cProperties.decode(res.getRow()));
            BigInteger st = new BigInteger(this.cProperties.utils.integerToByteArray(0));
            BigInteger e = new BigInteger(this.cProperties.utils.integerToByteArray(6));
            if(row.compareTo(st) >= 0 && row.compareTo(e) < 0) {
                return this.cProperties.decodeResult(res.getRow(), res);
            }
            else
                return new Result();
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

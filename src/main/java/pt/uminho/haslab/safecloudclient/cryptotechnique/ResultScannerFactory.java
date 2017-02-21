package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.client.ResultScanner;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;

/**
 * Created by rgmacedo on 2/21/17.
 */
public class ResultScannerFactory {

    public ResultScanner getResultScanner(CryptoTechnique.CryptoType cType, byte[] key, byte[] startRow, byte[] stopRow, ResultScanner rs) {
        switch (cType) {
            case STD:
                return new StandardResultScanner(key, startRow, stopRow, rs);
            case DET:
                return new DeterministicResultScanner();
            case OPE:
                return new OrderPreservingResultScanner();
            default:
                return null;
        }
    }

}

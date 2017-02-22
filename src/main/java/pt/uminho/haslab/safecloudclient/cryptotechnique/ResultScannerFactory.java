package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;

/**
 * Created by rgmacedo on 2/21/17.
 */
public class ResultScannerFactory {

    public ResultScanner getResultScanner(CryptoTechnique.CryptoType cType, CryptoProperties cp, byte[] startRow, byte[] stopRow, ResultScanner rs, Filter filter) {
        switch (cType) {
            case STD:
                return new StandardResultScanner(cp, startRow, stopRow, rs, filter);
            case DET:
                return new DeterministicResultScanner(cp, startRow, stopRow, rs);
            case OPE:
                return new OrderPreservingResultScanner(cp, rs);
            default:
                return null;
        }
    }

}

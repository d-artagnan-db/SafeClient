package pt.uminho.haslab.safecloudclient.cryptotechnique.resultscanner;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;

import java.io.IOException;
import java.util.Iterator;

/**
 * DeterministicResultScanner class.
 * ResultScanner instance, providing a secure ResultScanner with the DET CryptoBox.
 */
public class FormatPreservingResultScanner extends ResultScannerAbstratClass {

    public FormatPreservingResultScanner(CryptoProperties cp, byte[] startRow, byte[] endRow, ResultScanner encryptedScanner, Object filterResult) {
        super(cp, encryptedScanner, startRow, endRow, filterResult);
    }


    @Override
    public boolean digestor(byte[] content) {
        boolean digest = checkRow(content);

        if (hasFilter && digest) {
            if(this.filterType.equals("RowFilter")) {
                digest = checkValue(content);
            }
        }
        return digest;
    }
}

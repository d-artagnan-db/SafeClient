package pt.uminho.haslab.safeclient.secureTable.resultscanner;

import org.apache.hadoop.hbase.client.ResultScanner;
import pt.uminho.haslab.safeclient.secureTable.CryptoProperties;

/**
 * DeterministicResultScanner class.
 * ResultScanner instance, providing a secure ResultScanner with the DET CryptoBox.
 */
public class DeterministicResultScanner extends ResultScannerAbstratClass {

    public DeterministicResultScanner(CryptoProperties cp, byte[] startRow, byte[] endRow, ResultScanner encryptedScanner, Object filterResult) {
        super(cp, encryptedScanner, startRow, endRow, filterResult);
    }

    @Override
    public boolean digestor(byte[] row) {
        boolean digest = checkRow(row);
        if (hasFilter && digest) {
            if (this.filterType.equals("RowFilter")) {
                digest = checkValue(row);
            }
        }
        return digest;
    }
}

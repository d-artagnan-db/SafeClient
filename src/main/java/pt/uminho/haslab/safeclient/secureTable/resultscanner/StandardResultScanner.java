package pt.uminho.haslab.safeclient.secureTable.resultscanner;

import org.apache.hadoop.hbase.client.ResultScanner;
import pt.uminho.haslab.safeclient.secureTable.CryptoProperties;

/**
 * StandardResultScanner class.
 * ResultScanner instance, providing a secure ResultScanner with the STD CryptoBox.
 */
public class StandardResultScanner extends ResultScannerAbstratClass {
    public StandardResultScanner(CryptoProperties cp, byte[] startRow, byte[] endRow, ResultScanner encryptedScanner, Object filterResult) {
        super(cp, encryptedScanner, startRow, endRow, filterResult);
    }

    @Override
    public boolean digestor(byte[] content) {
        boolean digest = checkRow(content);

        if (hasFilter && digest) {
            if (this.filterType.equals("RowFilter")) {
                digest = checkValue(content);
            }
        }
        return digest;
    }

}

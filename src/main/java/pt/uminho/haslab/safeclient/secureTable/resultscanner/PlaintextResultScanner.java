package pt.uminho.haslab.safeclient.secureTable.resultscanner;

import org.apache.hadoop.hbase.client.ResultScanner;
import pt.uminho.haslab.safeclient.secureTable.CryptoProperties;

/**
 * PlaintextResultScanner class.
 * ResultScanner instance, providing a secure ResultScanner with the PLT CryptoBox.
 */
public class PlaintextResultScanner extends ResultScannerAbstratClass {

	public PlaintextResultScanner(CryptoProperties cp, ResultScanner encryptedScanner) {
		super(cp, encryptedScanner);
	}

	@Override
	public boolean digestor(byte[] content) {
		return true;
	}

}

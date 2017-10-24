package pt.uminho.haslab.safecloudclient.cryptotechnique.resultscanner;

import org.apache.hadoop.hbase.client.ResultScanner;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;

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

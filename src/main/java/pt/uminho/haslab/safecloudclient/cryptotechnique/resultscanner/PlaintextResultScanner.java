package pt.uminho.haslab.safecloudclient.cryptotechnique.resultscanner;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;

import java.io.IOException;
import java.util.Iterator;

/**
 * PlaintextResultScanner class.
 * ResultScanner instance, providing a secure ResultScanner with the PLT CryptoBox.
 */
public class PlaintextResultScanner implements ResultScanner {
	public CryptoProperties cProperties;
	public ResultScanner encryptedScanner;

	public PlaintextResultScanner(CryptoProperties cp, ResultScanner encryptedScanner) {
		this.cProperties = cp;
		this.encryptedScanner = encryptedScanner;
	}

	/**
	 * next() method : decode both row key and result set for the current Result object from the encryptedScanner.
	 * @return the original Result
	 * @throws IOException
	 */
	@Override
	public Result next() throws IOException {
		Result encryptedResult = this.encryptedScanner.next();
		if (encryptedResult != null) {
			byte[] row = this.cProperties.decodeRow(encryptedResult.getRow());
			return this.cProperties.decodeResult(row, encryptedResult);
		}
		else {
			return null;
		}
	}

	@Override
	public Result[] next(int i) throws IOException {
		return encryptedScanner.next(i);
	}

	@Override
	public void close() {
		this.encryptedScanner.close();
	}

	@Override
	public Iterator<Result> iterator() {
		return this.encryptedScanner.iterator();
	}
}

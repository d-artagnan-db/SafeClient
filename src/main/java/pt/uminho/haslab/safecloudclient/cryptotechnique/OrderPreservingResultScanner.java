package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Iterator;

/**
 * OrderPreservingResultScanner class.
 * ResultScanner instance, providing a secure ResultScanner with the OPE CryptoBox.
 */
public class OrderPreservingResultScanner implements ResultScanner {
	public CryptoProperties cProperties;
	public ResultScanner encryptedScanner;

	public OrderPreservingResultScanner(CryptoProperties cp, ResultScanner encryptedScanner) {
		this.cProperties = cp;
		this.encryptedScanner = encryptedScanner;
	}

	/**
	 * next() method : decode both row key and result set for the current Result object from the encryptedScanner
	 * @return the original Result
	 * @throws IOException
	 */
	public Result next() throws IOException {
		Result encryptedResult = this.encryptedScanner.next();
		if (encryptedResult != null) {
			byte[] decodedRow = this.cProperties.decodeRow(encryptedResult.getRow());
			return this.cProperties.decodeResult(decodedRow, encryptedResult);
		}
		else {
			return null;
		}
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

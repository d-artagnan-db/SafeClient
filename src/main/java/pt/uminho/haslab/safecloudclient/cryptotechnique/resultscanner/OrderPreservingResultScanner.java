package pt.uminho.haslab.safecloudclient.cryptotechnique.resultscanner;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;

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
	 * next() method : decode both Row-Key and result set for the current Result object from the encryptedScanner
	 * @return the original Result
	 * @throws IOException
	 */
	@Override
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

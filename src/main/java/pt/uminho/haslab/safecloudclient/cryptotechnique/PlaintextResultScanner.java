package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by rgmacedo on 3/15/17.
 */
public class PlaintextResultScanner implements ResultScanner {
	public CryptoProperties cProperties;
	public ResultScanner encryptedScanner;

	public PlaintextResultScanner(CryptoProperties cp, ResultScanner encryptedScanner) {
		this.cProperties = cp;
		this.encryptedScanner = encryptedScanner;
	}

	@Override
	public Result next() throws IOException {
		Result encryptedResult = this.encryptedScanner.next();
		if (encryptedResult != null) {
			return this.cProperties.decodeResult(encryptedResult.getRow(), encryptedResult);
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

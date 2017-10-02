package pt.uminho.haslab.safecloudclient.cryptotechnique.resultscanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * PlaintextResultScanner class.
 * ResultScanner instance, providing a secure ResultScanner with the PLT CryptoBox.
 */
public class PlaintextResultScanner implements ResultScanner {
	static final Log LOG = LogFactory.getLog(CryptoTable.class.getName());
	public CryptoProperties cProperties;
	public ResultScanner encryptedScanner;

	public PlaintextResultScanner(CryptoProperties cp, ResultScanner encryptedScanner) {
		this.cProperties = cp;
		this.encryptedScanner = encryptedScanner;
	}

	/**
	 * next() method : decode both Row-Key and result set for the current Result object from the encryptedScanner.
	 * @return the original Result
	 * @throws IOException
	 */
	@Override
	public Result next() throws IOException {
		Result encryptedResult = this.encryptedScanner.next();
		if (encryptedResult != null) {
			byte[] row = this.cProperties.decodeRow(encryptedResult.getRow());
			Result r = this.cProperties.decodeResult(row, encryptedResult);
			return r;
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
		try {
			List<Result> rs = new ArrayList<>();
			for(Result r = encryptedScanner.next(); r != null; r = encryptedScanner.next()) {
				byte[] row = this.cProperties.decodeRow(r.getRow());
				Result iteratorResult = this.cProperties.decodeResult(row, r);
				rs.add(iteratorResult);
            }

            return rs.iterator();

		} catch (Exception e) {
			LOG.error("PlaintextResultScanner Iterator Exception: "+e.getMessage());
		}
		return null;
	}
}

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
	 * next() method : decode both row key and result set for the current Result object from the encryptedScanner.
	 * @return the original Result
	 * @throws IOException
	 */
	@Override
	public Result next() throws IOException {
		LOG.debug("PlaintextResultScanner:Next():");
		Result encryptedResult = this.encryptedScanner.next();
		LOG.debug("PlaintextResultScanner:Next():Result:"+encryptedResult.toString());
		if (encryptedResult != null) {
			byte[] row = this.cProperties.decodeRow(encryptedResult.getRow());
			LOG.debug("PlaintextResultScanner:Next():Row:"+new String(row));
			Result r = this.cProperties.decodeResult(row, encryptedResult);
			LOG.debug("PlaintextResultScanner:Next():EncryptedResult:"+r.toString());
			return r;
		}
		else {
			return null;
		}
	}

	@Override
	public Result[] next(int i) throws IOException {
		LOG.debug("PlaintextResultScanner:Next(i):");
		return encryptedScanner.next(i);
	}

	@Override
	public void close() {
		this.encryptedScanner.close();
	}

	@Override
	public Iterator<Result> iterator() {
		try {
			LOG.debug("PlaintextResultScanner:Iterator:");
			List<Result> rs = new ArrayList<>();

			for(Result r = encryptedScanner.next(); r != null; r = encryptedScanner.next()) {
				byte[] row = this.cProperties.decodeRow(r.getRow());
				LOG.debug("PlaintextResultScanner:Next():Row:"+new String(row));
				Result iteratorResult = this.cProperties.decodeResult(row, r);
				LOG.debug("PlaintextResultScanner:Next():EncryptedResult:"+iteratorResult.toString());
				rs.add(iteratorResult);
            }

            LOG.debug("PlaintextResultScanner:Next():Iterator:Size:"+rs.size());

			Iterator<Result> i = rs.iterator();

			return i;

		} catch (Exception e) {
			LOG.error("PlaintextResultScanner Iterator Exception: "+e.getMessage());
		}
		return null;
	}
}

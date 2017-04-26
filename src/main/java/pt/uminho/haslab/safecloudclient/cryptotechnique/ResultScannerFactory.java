package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.client.ResultScanner;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;

/**
 * ResultScannerFactory class.
 * Factory pattern applied in order to provide a uniform getResultScanner operation
 */
public class ResultScannerFactory {

	public ResultScanner getResultScanner(CryptoTechnique.CryptoType cType, CryptoProperties cp, byte[] startRow, byte[] stopRow, ResultScanner rs, Object filterResult) {
		switch (cType) {
			case PLT :
				return new PlaintextResultScanner(cp, rs);
			case STD :
				return new StandardResultScanner(cp, startRow, stopRow, rs, filterResult);
			case DET :
				return new DeterministicResultScanner(cp, startRow, stopRow, rs, filterResult);
			case OPE :
				return new OrderPreservingResultScanner(cp, rs);
			default :
				return null;
		}
	}

}

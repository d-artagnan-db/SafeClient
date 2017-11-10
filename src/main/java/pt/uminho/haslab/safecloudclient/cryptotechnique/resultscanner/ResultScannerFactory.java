package pt.uminho.haslab.safecloudclient.cryptotechnique.resultscanner;

import org.apache.hadoop.hbase.client.ResultScanner;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;
import pt.uminho.haslab.safemapper.DatabaseSchema.CryptoType;

/**
 * ResultScannerFactory class.
 * Factory pattern applied in order to provide a uniform getResultScanner operation
 */
public class ResultScannerFactory {

	public ResultScanner getResultScanner(CryptoType cType, CryptoProperties cp, byte[] startRow, byte[] stopRow, ResultScanner rs, Object filterResult) {
		switch (cType) {
			case PLT :
				return new PlaintextResultScanner(cp, rs);
			case STD :
				return new StandardResultScanner(cp, startRow, stopRow, rs, filterResult);
			case DET :
				return new DeterministicResultScanner(cp, startRow, stopRow, rs, filterResult);
			case OPE :
				return new OrderPreservingResultScanner(cp, rs);
			case FPE :
				return new FormatPreservingResultScanner(cp, startRow, stopRow, rs, filterResult);
			default :
				return null;
		}
	}

}

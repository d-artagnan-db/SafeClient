package pt.uminho.haslab.safecloudclient.tests;

import java.math.BigInteger;
import java.util.List;
import pt.uminho.haslab.testingutils.ScanValidator;

public class ConcurrentPutScanWithStartKeyTest extends ConcurrentPutScanTest {

	public ConcurrentPutScanWithStartKeyTest(List<BigInteger> testingValues,
			List<List<BigInteger>> testingIdentifiers) throws Exception {
		super(testingValues, testingIdentifiers);
	}

	@Override
	protected byte[] getStartKey(ScanValidator validator) {
		return validator.generateStartKey();
	}

	@Override
	protected byte[] getStopKey(ScanValidator validator) {
		return null;
	}

}

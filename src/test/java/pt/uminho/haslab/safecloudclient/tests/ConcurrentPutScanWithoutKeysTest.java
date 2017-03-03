package pt.uminho.haslab.safecloudclient.tests;

import pt.uminho.haslab.testingutils.ScanValidator;

import java.math.BigInteger;
import java.util.List;

public class ConcurrentPutScanWithoutKeysTest extends ConcurrentPutScanTest {

	public ConcurrentPutScanWithoutKeysTest(List<BigInteger> testingValues,
			List<List<BigInteger>> testingIdentifiers) throws Exception {
		super(testingValues, testingIdentifiers);
	}

	@Override
	protected byte[] getStartKey(ScanValidator validator) {
		return null;
	}

	@Override
	protected byte[] getStopKey(ScanValidator validator) {
		return null;
	}

}

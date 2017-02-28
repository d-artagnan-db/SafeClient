package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Iterator;

/**
 * Created by rgmacedo on 2/21/17.
 */
public class DeterministicResultScanner implements ResultScanner {
	public ResultScanner scanner;
	public CryptoProperties cProperties;
	public byte[] startRow;
	public byte[] endRow;
	public boolean hasStartRow;
	public boolean hasEndRow;
	public boolean hasFilter;
	public CompareFilter.CompareOp compareOp;
	public byte[] compareValue;

	public DeterministicResultScanner(CryptoProperties cp, byte[] startRow,
			byte[] endRow, ResultScanner encryptedScanner, Object filterResult) {
		this.scanner = encryptedScanner;
		this.cProperties = cp;
		this.startRow = startRow;
		this.endRow = endRow;
		this.setFilters(startRow, endRow, filterResult);
	}

	public void setFilters(byte[] startRow, byte[] endRow, Object filter) {
		if (startRow.length != 0) {
			this.hasStartRow = true;
			this.startRow = startRow;
		} else {
			this.hasStartRow = false;
		}

		if (endRow.length != 0) {
			this.hasEndRow = true;
			this.endRow = endRow;
		} else {
			this.hasEndRow = false;
		}

		if (filter != null) {
			this.hasFilter = true;
			Object[] filterProperties = (Object[]) filter;
			this.compareOp = (CompareFilter.CompareOp) filterProperties[0];
			this.compareValue = (byte[]) filterProperties[1];
		} else {
			this.hasFilter = false;
		}
	}

	public boolean digestStartEndRow(byte[] row) {
		boolean digest;
		BinaryComparator binaryComparator = new BinaryComparator(row);
		if (hasStartRow && hasEndRow) {
			digest = (binaryComparator.compareTo(startRow) >= 0 && binaryComparator
					.compareTo(endRow) < 0);
		} else if (hasStartRow && !hasEndRow) {
			digest = (binaryComparator.compareTo(startRow) >= 0);
		} else if (hasEndRow) {
			digest = (binaryComparator.compareTo(endRow) < 0);
		} else {
			digest = true;
		}

		return digest;
	}

	public boolean digestFilter(byte[] row, byte[] value) {
		boolean digest = true;
		BinaryComparator binaryComparator = new BinaryComparator(row);

		switch (this.compareOp) {
			case EQUAL :
				digest = (binaryComparator.compareTo(value) == 0);
				break;
			case GREATER :
				digest = (binaryComparator.compareTo(value) > 0);
				break;
			case LESS :
				digest = (binaryComparator.compareTo(value) < 0);
				break;
			case GREATER_OR_EQUAL :
				digest = (binaryComparator.compareTo(value) >= 0);
				break;
			case LESS_OR_EQUAL :
				digest = (binaryComparator.compareTo(value) < 0);
				break;
		}
		return digest;
	}

	public Result next() throws IOException {
		Result res = this.scanner.next();
		boolean digest;
		if (res != null) {
			byte[] row = this.cProperties.decode(res.getRow());

			digest = digestStartEndRow(row);

			if (hasFilter && digest) {
				digest = digestFilter(row, this.compareValue);
			}

			if (digest)
				return this.cProperties.decodeResult(res.getRow(), res);
			else
				return new Result();

		} else
			return null;
	}

	public Result[] next(int i) throws IOException {
		return this.scanner.next(i);
	}

	public void close() {
		this.scanner.close();
	}

	public Iterator<Result> iterator() {
		return this.scanner.iterator();
	}
}

package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import pt.uminho.haslab.cryptoenv.Utils;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by rgmacedo on 2/21/17.
 */
public class StandardResultScanner implements ResultScanner {
	public ResultScanner scanner;
	public CryptoProperties cProperties;
	public byte[] startRow;
	public byte[] endRow;
	public boolean hasStartRow;
	public boolean hasEndRow;
	public boolean hasFilter;
	public CompareFilter.CompareOp compareOp;
	public byte[] compareValue;

	public StandardResultScanner(CryptoProperties cp, byte[] startRow,
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

	public int getPaddingSize(byte[] row) {
		int paddingSize = row.length;
		if (hasStartRow && hasEndRow) {
			if (startRow.length > paddingSize)
				paddingSize = startRow.length;
			if (endRow.length > paddingSize)
				paddingSize = endRow.length;
		} else if (hasStartRow && !hasEndRow) {
			if (startRow.length > paddingSize)
				paddingSize = startRow.length;
		} else if (hasEndRow) {
			if (endRow.length > paddingSize)
				paddingSize = endRow.length;
		}

		if (hasFilter) {
			if (compareValue.length > paddingSize)
				paddingSize = compareValue.length;
		}
		return paddingSize;
	}

	public boolean digestStartEndRow(int paddingSize, byte[] row) {
		boolean digest;
		Bytes.ByteArrayComparator byteArrayComparator = new Bytes.ByteArrayComparator();

		if (hasStartRow && hasEndRow) {
			row = Utils.addPadding(row, paddingSize);
			startRow = Utils.addPadding(startRow, paddingSize);
			endRow = Utils.addPadding(endRow, paddingSize);

			digest = (byteArrayComparator.compare(row, startRow) >= 0 && byteArrayComparator
					.compare(row, endRow) < 0);
		} else if (hasStartRow && !hasEndRow) {
			row = Utils.addPadding(row, paddingSize);
			startRow = Utils.addPadding(startRow, paddingSize);

			digest = (byteArrayComparator.compare(row, startRow) >= 0);
		} else if (hasEndRow) {
			row = Utils.addPadding(row, paddingSize);
			endRow = Utils.addPadding(endRow, paddingSize);

			digest = (byteArrayComparator.compare(row, endRow) < 0);
		} else {
			digest = true;
		}

		return digest;
	}

	public boolean digestFilter(int paddingSize, byte[] row, byte[] value) {
		boolean digest = true;
		Bytes.ByteArrayComparator byteArrayComparator = new Bytes.ByteArrayComparator();
		row = Utils.addPadding(row, paddingSize);
		value = Utils.addPadding(value, paddingSize);

		switch (this.compareOp) {
			case EQUAL :
				digest = (byteArrayComparator.compare(row, value) == 0);
				break;
			case GREATER :
				digest = (byteArrayComparator.compare(row, value) > 0);
				break;
			case LESS :
				digest = (byteArrayComparator.compare(row, value) < 0);
				break;
			case GREATER_OR_EQUAL :
				digest = (byteArrayComparator.compare(row, value) >= 0);
				break;
			case LESS_OR_EQUAL :
				digest = (byteArrayComparator.compare(row, value) < 0);
				break;
		}
		return digest;
	}

	public Result next() throws IOException {
		Result res = this.scanner.next();
		boolean digest;
		if (res != null) {
			byte[] row = this.cProperties.decode(res.getRow());
			int paddingSize = getPaddingSize(row);

			digest = digestStartEndRow(paddingSize, row);

			if (hasFilter && digest) {
				digest = digestFilter(paddingSize, row, this.compareValue);
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

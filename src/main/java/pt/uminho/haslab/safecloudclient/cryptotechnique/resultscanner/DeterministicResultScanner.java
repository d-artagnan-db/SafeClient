package pt.uminho.haslab.safecloudclient.cryptotechnique.resultscanner;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;

import java.io.IOException;
import java.util.Iterator;

/**
 * DeterministicResultScanner class.
 * ResultScanner instance, providing a secure ResultScanner with the DET CryptoBox.
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
//	public byte[] family;
//	public byte[] qualifier;
	public String filterType;

	public DeterministicResultScanner(CryptoProperties cp, byte[] startRow, byte[] endRow, ResultScanner encryptedScanner, Object filterResult) {
		this.scanner = encryptedScanner;
		this.cProperties = cp;
		this.startRow = startRow;
		this.endRow = endRow;
		this.setFilters(startRow, endRow, filterResult);
	}

	/**
	 * setFilter (startRow : byte[], endRow : byte[], filter : Object) method : set the start and stop rows and the filter properties into class variables
	 * @param startRow start row
	 * @param endRow stop row
	 * @param filter filter propeties. In case of RowFilter(CompareOperation,CompareValue). In case of SingleColumnValueFilter(Family,Qualififer,CompareOperation,CompareValue).
	 */
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
			if(filterProperties.length == 2) {
				this.filterType = "RowFilter";
				this.compareOp = (CompareFilter.CompareOp) filterProperties[0];
				this.compareValue = (byte[]) filterProperties[1];
			}
			else if(filterProperties.length == 4) {
				this.filterType = "SingleColumnValueFilter";
//				this.family = (byte[]) filterProperties[0];
//				this.qualifier = (byte[]) filterProperties[1];
				this.compareOp = (CompareFilter.CompareOp) filterProperties[2];
				this.compareValue = (byte[]) filterProperties[3];
			}
		} else {
			this.hasFilter = false;
		}
	}

	// public int getPaddingSize(byte[] row) {
	// int paddingSize = row.length;
	// if (hasStartRow && hasEndRow) {
	// if (startRow.length > paddingSize)
	// paddingSize = startRow.length;
	// if (endRow.length > paddingSize)
	// paddingSize = endRow.length;
	// } else if (hasStartRow && !hasEndRow) {
	// if (startRow.length > paddingSize)
	// paddingSize = startRow.length;
	// } else if (hasEndRow) {
	// if (endRow.length > paddingSize)
	// paddingSize = endRow.length;
	// }
	//
	// if (hasFilter) {
	// if (compareValue.length > paddingSize)
	// paddingSize = compareValue.length;
	// }
	// return paddingSize;
	// }

	/**
	 * digestStartEndRow(paddingSize : int, row : byte[]) method : check if a row key is between the start and stop rows
	 * @param paddingSize Not applicable
	 * @param row row key
	 * @return true if is comprehended between the two delimiter rows. Otherwise false.
	 */
	public boolean digestStartEndRow(int paddingSize, byte[] row) {
		boolean digest;
		Bytes.ByteArrayComparator byteArrayComparator = new Bytes.ByteArrayComparator();

		if (hasStartRow && hasEndRow) {
			// row = Utils.addPadding(row, paddingSize);
			// startRow = Utils.addPadding(startRow, paddingSize);
			// endRow = Utils.addPadding(endRow, paddingSize);

			digest = (byteArrayComparator.compare(row, startRow) >= 0 && byteArrayComparator.compare(row, endRow) < 0);
		} else if (hasStartRow && !hasEndRow) {
			// row = Utils.addPadding(row, paddingSize);
			// startRow = Utils.addPadding(startRow, paddingSize);

			digest = (byteArrayComparator.compare(row, startRow) >= 0);
		} else if (hasEndRow) {
			// row = Utils.addPadding(row, paddingSize);
			// endRow = Utils.addPadding(endRow, paddingSize);

			digest = (byteArrayComparator.compare(row, endRow) < 0);
		} else {
			digest = true;
		}

		return digest;
	}

	/**
	 * digestFilter(paddingSize : int, main : byte[], value : byte[]) method : check if a value match the filter properties
	 * @param paddingSize Not applicable
	 * @param main compare value
	 * @param value value to perform the comparison
	 * @return true if match the filter properties. Otherwise false.
	 */
	public boolean digestFilter(int paddingSize, byte[] main, byte[] value) {
		boolean digest = true;
		Bytes.ByteArrayComparator byteArrayComparator = new Bytes.ByteArrayComparator();
		// row = Utils.addPadding(row, paddingSize);
		// value = Utils.addPadding(value, paddingSize);

		switch (this.compareOp) {
			case EQUAL :
				digest = (byteArrayComparator.compare(main, value) == 0);
				break;
//			case GREATER :
//				digest = (byteArrayComparator.compare(main, value) > 0);
//				break;
//			case LESS :
//				digest = (byteArrayComparator.compare(main, value) < 0);
//				break;
//			case GREATER_OR_EQUAL :
//				digest = (byteArrayComparator.compare(main, value) >= 0);
//				break;
//			case LESS_OR_EQUAL :
//				digest = (byteArrayComparator.compare(main, value) < 0);
//				break;
			default:
				digest = false;
				break;
		}
		return digest;
	}

	/**
	 * next() method : decode both row key and result set for the current Result object from the encrypted scanner
	 * @return the original result
	 * @throws IOException
	 */
	public Result next() throws IOException {
		Result res = this.scanner.next();
		boolean digest;
		if (res != null) {
			byte[] row = this.cProperties.decodeRow(res.getRow());
			// int paddingSize = getPaddingSize(row);

			digest = digestStartEndRow(0, row);

			if (hasFilter && digest) {
				if(this.filterType.equals("RowFilter")) {
					digest = digestFilter(0, row, this.compareValue);
				}
//				else if(this.filterType.equals("SingleColumnValueFilter")) {
//					byte[] qualifierValue = this.cProperties.decodeValue(
//							this.family,
//							this.qualifier,
//							res.getValue(this.family, this.qualifier));
//
//					digest = digestFilter(0, qualifierValue, this.compareValue);
//				}
			}

			if (digest)
				return this.cProperties.decodeResult(row, res);
			else
				return Result.EMPTY_RESULT;

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

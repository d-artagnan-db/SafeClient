package pt.uminho.haslab.safecloudclient.deterministic;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class DetTable extends HTable {

	private final String key;
	private final String initVector;

	public DetTable() {
		key = "Bar12345Bar12345"; // 128 bit key
		initVector = "RandomInitVector"; // 16 bytes IV

	}

	private byte[] encode(byte[] content) {
		return Encryptor.encrypt(key, initVector, content);
	}

	private byte[] decode(byte[] content) {
		return Encryptor.encrypt(key, initVector, content);
	}

	private Result decodeResult(byte[] row, Result res) {

		List<Cell> cellList = new ArrayList<Cell>();
		while (res.advance()) {
			Cell cell = res.current();
			byte[] cf = CellUtil.cloneFamily(cell);
			byte[] cq = CellUtil.cloneQualifier(cell);
			byte[] value = CellUtil.cloneValue(cell);
			long timestamp = cell.getTimestamp();
			byte type = cell.getTypeByte();

			Cell decCell = CellUtil.createCell(row, cf, cq, timestamp, type,
					decode(value));
			cellList.add(decCell);
		}
		return Result.create(cellList);
	}
	@Override
	public Result get(Get get) throws IOException {

		try {
			byte[] row = get.getRow();
			Get nGet = new Get(encode(row));

			Result res = super.get(nGet);

			return decodeResult(row, res);
		} catch (IOException ex) {
			System.out.println("Exception in get " + ex.getMessage());
		}
		return null;
	}

	@Override
	public ResultScanner getScanner(Scan scan) throws IOException {
		byte[] startRow = scan.getStartRow();
		byte[] stopRow = scan.getStopRow();
		Scan newScan = null;
		if (startRow.length != 0 && stopRow.length != 0) {
			newScan = new Scan(encode(startRow), encode(stopRow));
		} else if (startRow.length != 0 && stopRow.length == 0) {
			newScan = new Scan(encode(startRow));
		} else if (startRow.length == 0 && stopRow.length == 0) {
			newScan = new Scan();
		}

		ResultScanner encScan = super.getScanner(newScan);

		return new DetResultScanner(encScan);
	}

	public class DetResultScanner implements ResultScanner {
		public ResultScanner encryptedScanner;

		public DetResultScanner(ResultScanner encScanner) {
			this.encryptedScanner = encScanner;
		}

		public Result next() throws IOException {
			Result res = encryptedScanner.next();
			return decodeResult(decode(res.getRow()), res);
		}

		public Result[] next(int i) throws IOException {
			return encryptedScanner.next(i);
		}

		public void close() {
			encryptedScanner.close();
		}

		public Iterator<Result> iterator() {
			return encryptedScanner.iterator();
		}

	}

	@Override
	public void put(Put put) {
		try {
			byte[] row = put.getRow();
			Put nPut = new Put(encode(row));
			CellScanner cs = put.cellScanner();

			while (cs.advance()) {
				Cell cell = cs.current();
				nPut.add(CellUtil.cloneFamily(cell),
						CellUtil.cloneQualifier(cell),
						encode(CellUtil.cloneValue(cell)));
			}
			super.put(nPut);
		} catch (IOException ex) {
			System.out.println("Exception in put get " + ex.getMessage());
		}

	}

}

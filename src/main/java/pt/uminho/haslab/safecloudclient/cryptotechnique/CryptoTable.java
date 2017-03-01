package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * Created by rgmacedo on 2/20/17.
 */
public class CryptoTable extends HTable {

	public CryptoProperties cryptoProperties;
	public ResultScannerFactory resultScannerFactory;

	public CryptoTable(CryptoTechnique.CryptoType cType) {
		this.cryptoProperties = new CryptoProperties(cType, 23);
		this.resultScannerFactory = new ResultScannerFactory();
	}

	public CryptoTable(Configuration conf, String tableName,
			CryptoTechnique.CryptoType cType) throws IOException {
		super(conf, TableName.valueOf(tableName));
		this.cryptoProperties = new CryptoProperties(cType, 23);
		this.resultScannerFactory = new ResultScannerFactory();
	}

	@Override
	public void put(Put put) {
		try {
			byte[] row = put.getRow();
			Put encPut = new Put(this.cryptoProperties.encode(row));

			CellScanner cs = put.cellScanner();

			while (cs.advance()) {
				Cell cell = cs.current();
				encPut.add(CellUtil.cloneFamily(cell),
						CellUtil.cloneQualifier(cell),
						CellUtil.cloneValue(cell));
			}
			super.put(encPut);

		} catch (IOException e) {
			System.out.println("CryptoTable: Exception in put method - "
					+ e.getMessage());
		}
	}

	@Override
	public Result get(Get get) {
		Scan getScan = new Scan();
		Result getResult = null;

		try {
			byte[] row = get.getRow();
			switch (this.cryptoProperties.cType) {
				case STD :
					ResultScanner encScan = super.getScanner(getScan);
					for (Result r = encScan.next(); r != null; r = encScan
							.next()) {
						Result res = this.cryptoProperties.decodeResult(
								r.getRow(), r);
						byte[] aux = res.getRow();

						if (Arrays.equals(row, aux)) {
							getResult = res;
							break;
						}
					}
					return getResult;
				case DET :
				case OPE :
					Get encGet = new Get(this.cryptoProperties.encode(row));
					Result res = super.get(encGet);
					if (!res.isEmpty()) {
						getResult = this.cryptoProperties.decodeResult(
								res.getRow(), res);
					}
					return getResult;
				default :
					break;
			}

		} catch (IOException e) {
			System.out.println("CryptoTable: Exception in get method. "
					+ e.getMessage());
		}
		return getResult;
	}

	@Override
	public void delete(Delete delete) {
		Scan deleteScan = new Scan();
		try {
			byte[] row = delete.getRow();
			switch (this.cryptoProperties.cType) {
				case STD :
					ResultScanner encScan = super.getScanner(deleteScan);
					for (Result r = encScan.next(); r != null; r = encScan
							.next()) {
						Result res = this.cryptoProperties.decodeResult(
								r.getRow(), r);
						byte[] resultValue = res.getRow();

						if (Arrays.equals(row, resultValue)) {
							Delete del = new Delete(r.getRow());
							super.delete(del);
							System.out.println("Row deleted: "
									+ new String(row));
						}
					}
					break;
				case DET :
				case OPE :
					Delete encDelete = new Delete(
							this.cryptoProperties.encode(row));
					super.delete(encDelete);
					System.out.println("Row deleted: " + new String(row));
					break;
				default :
					break;
			}
		} catch (Exception e) {
			System.out.println("CryptoTable: Exception in delete method. "
					+ e.getMessage());
		}
	}

	@Override
	public ResultScanner getScanner(Scan scan) {
		try {
			byte[] startRow = scan.getStartRow();
			byte[] endRow = scan.getStopRow();

			Scan encScan = this.cryptoProperties.encryptedScan(scan);
			ResultScanner encryptedResultScanner = super.getScanner(encScan);

			return this.resultScannerFactory.getResultScanner(
					this.cryptoProperties.cType, this.cryptoProperties,
					startRow, endRow, encryptedResultScanner,
					this.cryptoProperties.parseFilter((RowFilter) scan
							.getFilter()));
		} catch (Exception e) {
			System.out.println("CryptoTable: Exception in scan method. "
					+ e.getMessage());
		}
		return null;
	}

}

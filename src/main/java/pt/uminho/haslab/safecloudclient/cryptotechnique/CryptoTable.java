package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import pt.uminho.haslab.safecloudclient.schema.SchemaParser;
import pt.uminho.haslab.safecloudclient.schema.TableSchema;

import java.io.IOException;
import java.util.Arrays;

public class CryptoTable extends HTable {
	static final Log LOG = LogFactory.getLog(CryptoTable.class.getName());

	public CryptoProperties cryptoProperties;
	public ResultScannerFactory resultScannerFactory;
	public TableSchema tableSchema;

	public CryptoTable(Configuration conf, String tableName, String schemaFilename) throws IOException {
		super(conf, TableName.valueOf(tableName));
		this.tableSchema = this.init(schemaFilename);
		this.cryptoProperties = new CryptoProperties(this.tableSchema);
		this.resultScannerFactory = new ResultScannerFactory();
	}

	public CryptoTable(Configuration conf, String tableName, TableSchema schema) throws IOException {
		super(conf, TableName.valueOf(tableName));
		this.tableSchema = schema;
		this.cryptoProperties = new CryptoProperties(this.tableSchema);
		this.resultScannerFactory = new ResultScannerFactory();
	}

	public TableSchema init(String filename) {
		SchemaParser schemaParser = new SchemaParser();
		schemaParser.parse(filename);
		System.out.println(schemaParser.tableSchema.toString());
		return schemaParser.getTableSchema();
	}

	@Override
	public void put(Put put) {
		try {
			byte[] row = put.getRow();

			Put encPut = new Put(this.cryptoProperties.encodeRow(row));

			CellScanner cs = put.cellScanner();
			while (cs.advance()) {
				Cell cell = cs.current();
				encPut.add(
						CellUtil.cloneFamily(cell),
						CellUtil.cloneQualifier(cell),
						this.cryptoProperties.encodeValue(
								CellUtil.cloneFamily(cell),
								CellUtil.cloneQualifier(cell),
								CellUtil.cloneValue(cell)));
			}
			super.put(encPut);

		} catch (IOException e) {
			LOG.error("Exception in put method. " + e.getMessage());
		}
	}

	@Override
	public Result get(Get get) {
		Scan getScan = new Scan();
		Result getResult = Result.EMPTY_RESULT;

		try {
			byte[] row = get.getRow();
			switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
				case PLT :
					Result pltResult = super.get(get);
					return this.cryptoProperties.decodeResult(get.getRow(), pltResult);
				case STD :
					ResultScanner encScan = super.getScanner(getScan);
					for (Result r = encScan.next(); r != null; r = encScan.next()) {
						byte[] aux = this.cryptoProperties.decodeRow(r.getRow());

						if (Arrays.equals(row, aux)) {
							getResult = this.cryptoProperties.decodeResult(row, r);
							break;
						}
					}
					return getResult;
				case DET :
				case OPE :
					Get encGet = new Get(this.cryptoProperties.encodeRow(row));
					Result res = super.get(encGet);

					if (!res.isEmpty()) {
						getResult = this.cryptoProperties.decodeResult(row, res);
					}

					return getResult;
				default :
					break;
			}

		} catch (IOException e) {
			LOG.error("Exception in get method. " + e.getMessage());
		}
		LOG.debug("Going to return result " + getResult);
		return getResult;
	}

	@Override
	public void delete(Delete delete) {
		Scan deleteScan = new Scan();
		try {
			byte[] row = delete.getRow();
			switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
				case PLT :
					super.delete(delete);
					break;
				case STD :
					ResultScanner encScan = super.getScanner(deleteScan);
					for (Result r = encScan.next(); r != null; r = encScan.next()) {
						byte[] resultValue = this.cryptoProperties.decodeRow(r.getRow());

						if (Arrays.equals(row, resultValue)) {
							Delete del = new Delete(r.getRow());
							super.delete(del);
						}
					}
					break;
				case DET :
				case OPE :
					Delete encDelete = new Delete(this.cryptoProperties.encodeRow(row));
					super.delete(encDelete);
					break;
				default :
					break;
			}
		} catch (Exception e) {
			LOG.error("Exception in delete method. " + e.getMessage());
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
					this.cryptoProperties.verifyFilterCryptoType(scan),
					this.cryptoProperties,
					startRow,
					endRow,
					encryptedResultScanner,
					this.cryptoProperties.parseFilter(scan.getFilter()));

		} catch (Exception e) {
			LOG.error("Exception in scan method. " + e.getMessage());
		}
		return null;
	}

}

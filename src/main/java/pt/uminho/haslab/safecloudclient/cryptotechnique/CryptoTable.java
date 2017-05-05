package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.schema.SchemaParser;
import pt.uminho.haslab.safecloudclient.schema.TableSchema;

import java.io.IOException;
import java.util.*;

/**
 * CryptoTable class.
 * Provides a secure extended version of an HBase HTable
 */
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

	/**
	 * init(String filename) method : read and creates a database secure schema
	 * @param filename path to the database schema
	 * @return TableSchema object
	 */
	public TableSchema init(String filename) {
		SchemaParser schemaParser = new SchemaParser();
		schemaParser.parse(filename);
//		System.out.println(schemaParser.tableSchema.toString());
		return schemaParser.getTableSchema();
	}

	/**
	 * put(Put put) method : secure put/update method.
	 * The original put object sends a set of qualifiers and values to insert in the database system.
	 * Before the insertion both key and values are encrypted, following the database schema specifications.
	 * In case of OPE CryptoBox, an additional qualifier is created and stores the respective value encrypted with the STD CryptoBox
	 * @param put original put object that contains the key, values, qualifiers, ...
	 */
	@Override
	public void put(Put put) {
		try {
			byte[] row = put.getRow();
//			Encode the row key
			Put encPut = new Put(this.cryptoProperties.encodeRow(row));
//			System.out.println("Going to put (plaintext): "+Arrays.toString(row));
			CellScanner cs = put.cellScanner();
			while (cs.advance()) {
				Cell cell = cs.current();
				byte[] family = CellUtil.cloneFamily(cell);
				byte[] qualifier = CellUtil.cloneQualifier(cell);
				byte[] value = CellUtil.cloneValue(cell);

				boolean verifyProperty = false;
				String qualifierString = new String(qualifier);
				String opeValues = "_STD";

//				Verify if the actual qualifier corresponds to the supporting qualifier (<qualifier>_STD)
				if (qualifierString.length() >= opeValues.length()) {
					verifyProperty = qualifierString.substring(qualifierString.length() - opeValues.length(), qualifierString.length()).equals(opeValues);
				}
				if (!verifyProperty) {
//					Encode the original value with the corresponding CryptoBox
					encPut.add(
							family,
							qualifier,
							this.cryptoProperties.encodeValue(
									family,
									qualifier,
									value));

//					If the actual qualifier CryptoType is equal to OPE, encode the same value with STD CryptoBox
					if (tableSchema.getCryptoTypeFromQualifier(new String(family), qualifierString) == CryptoTechnique.CryptoType.OPE) {
						encPut.add(
								family,
								(qualifierString + opeValues).getBytes(),
								this.cryptoProperties.encodeValue(
										family,
										(qualifierString + opeValues).getBytes(),
										value)
						);
					}

				}
			}

//			System.out.println("Going to put (ciphertext): "+Arrays.toString(encPut.getRow()));
			super.put(encPut);

		} catch (IOException e) {
			System.out.println("Exception in put method. " + e.getMessage());
			LOG.error("Exception in put method. " + e.getMessage());
		}
	}

	/**
	 * get(Get get) method : secure get method.
	 * The original get object sets the row key to search in the database system. Before the get operation, the row key
	 * is encrypted accordingly the respective CryptoBox and its issued. After the server response, all the values must be
	 * decoded with the respective CryptoBox, resulting in the original values stored by the user.
	 * @param get original get object that contains the key to perform the operation.
	 * @return Result containing the plaintext value of the get result.
	 */
	@Override
	public Result get(Get get) {
		Scan getScan = new Scan();
		Result getResult = Result.EMPTY_RESULT;

		try {
			byte[] row = get.getRow();
//			Verify the row key CryptoBox
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
				case FPE :
					String opeValue = "_STD";
					Get encGet = new Get(this.cryptoProperties.encodeRow(row));

//					System.out.println("Going to get (ciphertext): "+Arrays.toString(encGet.getRow()));
					Map<byte[],List<byte[]>> columns = this.cryptoProperties.getFamiliesAndQualifiers(get.getFamilyMap());

					for(byte[] f : columns.keySet()) {
						List<byte[]> qualifiersTemp = columns.get(f);
						for(byte[] q : qualifiersTemp) {
							encGet.addColumn(f, q);
						}
					}

					Result res = super.get(encGet);

					if (!res.isEmpty()) {
						getResult = this.cryptoProperties.decodeResult(row, res);
					}

//					System.out.println("Going to get (plaintext): "+Arrays.toString(getResult.getRow())+" - "+Arrays.toString(getResult.getValue("Physician".getBytes(), "Physician ID".getBytes())));
					return getResult;
				default :
					break;
			}

		} catch (IOException e) {
			System.out.println("Exception in get method. " + e.getMessage());
			LOG.error("Exception in get method. " + e.getMessage());
		}
		return getResult;
	}

	/**
	 * delete(Delete delete) method : secure delete method.
	 * The original delete object sets the row key to search in the database system. Before the delete operation, the row key
	 * is encrypted accordingly the respective CryptoBox and its issued.
	 * @param delete original get object that contains the key to perform the operation.
	 */
	@Override
	public void delete(Delete delete) {
		Scan deleteScan = new Scan();
		try {
			byte[] row = delete.getRow();
//			Verify the row key CryptoBox
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
				case FPE :
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

	/**
	 * getScanner(Scan scan) method : secure scan and filter operations
	 * This operations provides the secure scan and filter operations over the database. Encrypting both start row, stop row
	 * and compare value.
	 * @param scan scan object that provides the necessary filter and scan parameters.
	 * @return resulting values that pass the filter parameters. The values are still encrypted.
	 */
	@Override
	public ResultScanner getScanner(Scan scan) {
		try {
			byte[] startRow = scan.getStartRow();
			byte[] endRow = scan.getStopRow();

//			Transform the original object in an encrypted scan.
			Scan encScan = this.cryptoProperties.encryptedScan(scan);
			ResultScanner encryptedResultScanner = super.getScanner(encScan);

//			Return the corresponding result scanner to decrypt the resulting set of values
			return this.resultScannerFactory.getResultScanner(
					this.cryptoProperties.verifyFilterCryptoType(scan),
					this.cryptoProperties,
					startRow,
					endRow,
					encryptedResultScanner,
					this.cryptoProperties.parseFilter(scan.getFilter()));

		} catch (Exception e) {
			System.out.println("Exception in scan method. "+e.getMessage());
			LOG.error("Exception in scan method. " + e.getMessage());
		}
		return null;
	}

}

package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Pair;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.schema.SchemaParser;
import pt.uminho.haslab.safecloudclient.schema.TableSchema;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.Charset;
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
	public HTableFeaturesUtils htableUtils;

	public CryptoTable(Configuration conf, String tableName, String schemaFilename) throws IOException {
		super(conf, TableName.valueOf(tableName));
		this.tableSchema = this.init(schemaFilename, tableName);
		this.cryptoProperties = new CryptoProperties(this.tableSchema);
		this.resultScannerFactory = new ResultScannerFactory();
		this.htableUtils = new HTableFeaturesUtils(this.cryptoProperties);
	}

	public CryptoTable(Configuration conf, String tableName, TableSchema schema) throws IOException {
		super(conf, TableName.valueOf(tableName));
		this.tableSchema = schema;
		this.cryptoProperties = new CryptoProperties(this.tableSchema);
		this.resultScannerFactory = new ResultScannerFactory();
		this.htableUtils = new HTableFeaturesUtils(this.cryptoProperties);
	}

	/**
	 * init(String filename) method : read and creates a database secure schema
	 * @param filename path to the database schema
	 * @return TableSchema object
	 */
	public TableSchema init(String filename, String tablename) {
		if (filename == null) {
			throw new NullPointerException("Schema file name cannot be null.");
		}

		SchemaParser schemaParser = new SchemaParser();
		schemaParser.parse(filename);
//		System.out.println(schemaParser.tableSchema.toString());
		return schemaParser.getTableSchema(tablename);
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
			if(row.length == 0) {
				throw new NullPointerException("Row Key cannot be null.");
			}
//			Encode the row key
			Put encPut = new Put(this.cryptoProperties.encodeRow(row));
			System.out.println("Put(before): "+encPut.toString());
//			System.out.println("Going to put (plaintext): "+Arrays.toString(row));
			this.htableUtils.encryptCells(put.cellScanner(), this.tableSchema, encPut, this.cryptoProperties);
			System.out.println("Put(after): "+encPut.toString());

			super.put(encPut);

		} catch (IOException e) {
			LOG.error("Exception in put method. " + e.getMessage());
		}
	}

	@Override
	public void put(List<Put> puts) {
		try {
			List<Put> encryptedPuts = new ArrayList<>(puts.size());
			for(Put p : puts) {
				byte[] row = p.getRow();
				if(row.length == 0) {
					throw new NullPointerException("Row Key cannot be null.");
				}
//				Encode the row key
				Put encPut = new Put(this.cryptoProperties.encodeRow(row));
//				System.out.println("Going to put (plaintext): "+Arrays.toString(row));
				this.htableUtils.encryptCells(p.cellScanner(), this.tableSchema, encPut, this.cryptoProperties);

				encryptedPuts.add(encPut);
			}

			super.put(encryptedPuts);

		} catch (InterruptedIOException | RetriesExhaustedWithDetailsException e) {
			LOG.error("Exception in put (list<Put> puts) method. " + e.getMessage());
			System.out.println("Exception in put (list<Put> puts) method. " + e.getMessage());
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
			if(row.length == 0) {
				throw new NullPointerException("Row Key cannot be null.");
			}

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
					Get encGet = new Get(this.cryptoProperties.encodeRow(row));

					Map<byte[],List<byte[]>> columns = this.cryptoProperties.getFamiliesAndQualifiers(get.getFamilyMap());
					for(byte[] f : columns.keySet()) {
						for(byte[] q : columns.get(f)){
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

	@Override
	public Result[] get(List<Get> gets) {
		Result[] results = new Result[gets.size()];
		List<Get> encryptedGets = new ArrayList<>(gets.size());


		for(Get g : gets) {
			byte[] row = g.getRow();
			if(row.length == 0) {
				throw new NullPointerException("Row Key cannot be null.");
			}

//			First phase: Encrypt all get objects
			switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
				case PLT :
				case STD :
					encryptedGets.add(g);
					break;
				case DET :
				case OPE :
				case FPE :
					Get encGet = new Get(this.cryptoProperties.encodeRow(row));

					Map<byte[],List<byte[]>> columns = this.cryptoProperties.getFamiliesAndQualifiers(g.getFamilyMap());

					for(byte[] f : columns.keySet()) {
						for(byte[] q : columns.get(f)){
							encGet.addColumn(f, q);
						}
					}

					encryptedGets.add(encGet);

					break;
				default :
					break;
			}
		}

//		Second phase: call super() to batch the Get's List
		try {
			switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
				case PLT :
					results = super.get(encryptedGets);
					break;
				case STD :
					for(int i = 0; i < encryptedGets.size(); i++) {
						byte[] row = encryptedGets.get(i).getRow();
						ResultScanner encScan = super.getScanner(new Scan());
						for (Result r = encScan.next(); r != null; r = encScan.next()) {
							byte[] aux = this.cryptoProperties.decodeRow(r.getRow());

//							Third phase: decode result
							if (Arrays.equals(row, aux)) {
								results[i] = this.cryptoProperties.decodeResult(row, r);
								break;
							}
						}

					}
					break;
				case DET:
				case OPE:
				case FPE:
					Result[] encryptedResults = super.get(encryptedGets);
					for(int i = 0; i < encryptedResults.length; i++) {
//						Third phase: decode result
						if(!encryptedResults[i].isEmpty()) {
							byte[] row = gets.get(i).getRow();
							results[i] = this.cryptoProperties.decodeResult(row, encryptedResults[i]);
						}
					}
					break;
				default:
					break;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}


		return results;
	}

//	TODO UnitTest
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
			if(row.length == 0) {
				throw new NullPointerException("Row Key cannot be null.");
			}

			System.out.println("Delete: "+delete.toString());
			List<String> cellsToDelete = this.htableUtils.deleteCells(delete.cellScanner());

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
							this.htableUtils.wrapDeletedCells(cellsToDelete, del);
							super.delete(del);
							break;
						}
					}
					break;
				case DET :
				case OPE :
				case FPE :
					Delete encDelete = new Delete(this.cryptoProperties.encodeRow(row));
					System.out.println("Delete (before wrapping): "+encDelete.toString());
					this.htableUtils.wrapDeletedCells(cellsToDelete, encDelete);
					System.out.println("Delete (after wrapping): "+encDelete.toString());
					super.delete(encDelete);
					break;
				default :
					break;
			}
		} catch (Exception e) {
			LOG.error("Exception in delete method. " + e.getMessage());
		}
	}

//	TODO UnitTest
	@Override
	public void delete(List<Delete> deletes) {
		try {
			List<Delete> encryptedDeletes = new ArrayList<>(deletes.size());
			for (Delete del : deletes) {
				byte[] row = del.getRow();
				if (row.length == 0) {
					throw new NullPointerException("Row Key cannot be null.");
				}

				List<String> cellsToDelete = this.htableUtils.deleteCells(del.cellScanner());

//			Verify the row key CryptoBox
				switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
					case PLT:
						encryptedDeletes.add(del);
						break;
					case STD:
						ResultScanner encScan = super.getScanner(new Scan());
						for (Result r = encScan.next(); r != null; r = encScan.next()) {
							byte[] resultValue = this.cryptoProperties.decodeRow(r.getRow());

							if (Arrays.equals(row, resultValue)) {
								Delete encryptedDelete = new Delete(r.getRow());
								this.htableUtils.wrapDeletedCells(cellsToDelete, encryptedDelete);
								encryptedDeletes.add(encryptedDelete);
								break;
							}
						}
						break;
					case DET:
					case OPE:
					case FPE:
						Delete encryptedDelete = new Delete(this.cryptoProperties.encodeRow(row));
						System.out.println("Delete (before wrapping): " + encryptedDelete.toString());
						this.htableUtils.wrapDeletedCells(cellsToDelete, encryptedDelete);
						System.out.println("Delete (after wrapping): " + encryptedDelete.toString());
						encryptedDeletes.add(encryptedDelete);
						break;
					default:
						break;
				}
			}

			super.delete(encryptedDeletes);

		} catch(IOException e) {
			LOG.error("Exception in delete (list<Delete> deletes) method. " + e.getMessage());
			System.out.println("Exception in delete (list<Delete> deletes) method. " + e.getMessage());
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
			Scan encScan = this.htableUtils.encryptedScan(scan);
			ResultScanner encryptedResultScanner = super.getScanner(encScan);

//			Return the corresponding result scanner to decrypt the resulting set of values
			return this.resultScannerFactory.getResultScanner(
					this.htableUtils.verifyFilterCryptoType(scan),
					this.cryptoProperties,
					startRow,
					endRow,
					encryptedResultScanner,
					this.htableUtils.parseFilter(scan.getFilter()));

		} catch (Exception e) {
			System.out.println("Exception in scan method. "+e.getMessage());
			LOG.error("Exception in scan method. " + e.getMessage());
		}
		return null;
	}

//	TODO complete UnitTest
	@Override
	public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) {
		boolean operationPerformed = false;
		System.out.println("Secure Check and Put");
		try {
			if(row.length == 0) {
				throw new NullPointerException("Row Key cannot be null.");
			}
			if(family != null && qualifier != null) {
				switch (this.tableSchema.getKey().getCryptoType()) {
					case PLT:
						Put encPut;
						if(!put.isEmpty()) {
							encPut = new Put(put.getRow());
							this.htableUtils.encryptCells(put.cellScanner(), this.tableSchema, encPut, this.cryptoProperties);
						}
						else {
							throw new NullPointerException("Put object cannot be null");
						}

						super.checkAndPut(row, family, qualifier, value, encPut);
						break;
					case STD:
//						step 1 : get all stored values
						ResultScanner rs = super.getScanner(new Scan());
//						step 2 : check if specified row exists
						for(Result r = rs.next(); r != null; r = rs.next()) {
							byte[] resultRow = this.cryptoProperties.decodeRow(r.getRow());
							if (Arrays.equals(row, resultRow)) {
//								Get the stored value for the specified family and qualifier and check if it's equal to a given value
								byte[] encryptedValue = r.getValue(family, qualifier);
								if(encryptedValue != null && encryptedValue.length > 0) {
									byte[] resultValue = this.cryptoProperties.decodeValue(family, qualifier, encryptedValue);
									if(Arrays.equals(value, resultValue)) {
//										If the values match, build and encrypted put
										Put encryptedPut;
										if(!put.isEmpty()) {
											encryptedPut = new Put(this.cryptoProperties.encodeRow(put.getRow()));
											this.htableUtils.encryptCells(put.cellScanner(), this.tableSchema, encryptedPut, this.cryptoProperties);
										}
										else {
											throw new NullPointerException("Put object cannot be null");
										}
//										Call super
										operationPerformed = super.checkAndPut(r.getRow(), family, qualifier, encryptedValue, encryptedPut);
									}
								} else {
									throw new NullPointerException("No matching Cell for the family and qualifier specified.");
								}
								break;
							}
						}
						break;
					case DET:
					case OPE:
					case FPE:
//						step 1 : encrypt row and value
						System.out.println("Entrou no DET: ");
						byte[] encryptedRow = this.cryptoProperties.encodeRow(row);
						System.out.println("Encode row "+Arrays.toString(encryptedRow));
						byte[] encryptedValue = this.cryptoProperties.encodeValue(family, qualifier, value);
						System.out.println("Encrypted value: "+Arrays.toString(encryptedValue));
//						step 2 : encrypt put
						Put encryptedPut;
						if(!put.isEmpty()) {
							encryptedPut = new Put(this.cryptoProperties.encodeRow(put.getRow()));
							this.htableUtils.encryptCells(put.cellScanner(), this.tableSchema, encryptedPut, this.cryptoProperties);
						}
						else {
							throw new NullPointerException("Put object cannot be null");
						}
						System.out.println("Encrypted Put: "+encryptedPut.toString());
//						step 3 : call super
						operationPerformed = super.checkAndPut(encryptedRow, family, qualifier, encryptedValue, encryptedPut);
						System.out.println("Operation Performed "+operationPerformed);
						break;
					default:
						break;
				}
			}
			else {
				throw new NullPointerException("Column family and column qualifier cannot be null.");
			}
		} catch (IOException e) {
			System.out.println("Exception in checkAndPut method. "+e.getMessage());
			LOG.error("Exception in checkAndPut method. "+e.getMessage());
		}
		return operationPerformed;
	}

//	TODO UnitTest
	@Override
	public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) {
		long operationValue = 0;
		try {
			switch (this.tableSchema.getCryptoTypeFromQualifier(new String(family), new String(qualifier))) {
			case PLT:
				operationValue = super.incrementColumnValue(this.cryptoProperties.encodeRow(row), family, qualifier, amount);
				break;
			case STD:
			case DET:
			case OPE:
			case FPE:
				throw new UnsupportedOperationException("Secure operation not supported. Only for vanilla operations.");
			default:
				break;
			}
		} catch (IOException e) {
			System.out.println("Exception in incrementColumnValue method. "+e.getMessage());
			LOG.error("Exception in incrementColumnValue method. "+e.getMessage());
		}
		return operationValue;
	}

	@Override
	public List<HRegionLocation> getRegionsInRange(byte[] startKey, byte[] endKey) {
		return null;
	}

	@Override
	public Result getRowOrBefore(byte[] row, byte[] family) {
		return null;
	}

	@Override
	public Pair<byte[][], byte[][]> getStartEndKeys() {
		return null;
	}

	@Override
	public byte[][] getStartKeys() {
		return null;
	}

}

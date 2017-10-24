package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.cryptotechnique.resultscanner.ResultScannerFactory;
import pt.uminho.haslab.safecloudclient.cryptotechnique.securefilterfactory.SecureFilterConverter;
import pt.uminho.haslab.safecloudclient.schema.Family;
import pt.uminho.haslab.safecloudclient.schema.Key;
import pt.uminho.haslab.safecloudclient.schema.SchemaParser;
import pt.uminho.haslab.safecloudclient.schema.TableSchema;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * CryptoTable class.
 * Provides a secure extended version of an HBase HTable
 */
public class CryptoTable extends HTable {
	static final Log LOG = LogFactory.getLog(CryptoTable.class.getName());

	public CryptoProperties cryptoProperties;
	public ResultScannerFactory resultScannerFactory;
	public HTableFeaturesUtils htableUtils;
	public SecureFilterConverter secureFilterConverter;

	private static Map<String, Object> databaseDefaultProperties;
	private static Map<String,TableSchema> databaseSchema;
	private static final Lock lock = new ReentrantLock();
	private static boolean parsingComplete = false;

	private static boolean keyAcknowledgement = false;
	private static byte[] cryptographicKey;

	public TableSchema tableSchema;


	public CryptoTable(Configuration conf, String tableName) throws IOException {
		super(conf, TableName.valueOf(tableName));
		String schemaProperty = conf.get("schema");

		this.setSchema(conf, schemaProperty, tableName);

//		While the cryptographic keys management is not defined, read keys from specific files
//		arrange cryptographic keys properties
		this.setCryptographicKey(conf);

		for(CryptoTechnique.CryptoType cryptoType : this.tableSchema.getEnabledCryptoTypes()) {
			this.cryptoProperties.setKey(cryptoType, cryptographicKey);
		}

//		this is common for both cases
		this.resultScannerFactory = new ResultScannerFactory();
		this.secureFilterConverter = new SecureFilterConverter(this.cryptoProperties);
		this.htableUtils = new HTableFeaturesUtils(this.cryptoProperties, this.secureFilterConverter);
	}

	private void setSchema(Configuration conf, String schemaProperty, String tableName) throws FileNotFoundException {
		if(schemaProperty != null && !schemaProperty.isEmpty()) {
			while(!parsingComplete) {
				try {
					lock.lock();
					if (!parsingComplete) {
						File file = new File(schemaProperty);
						LOG.debug("Thread-"+Thread.currentThread().getId() + ":build database schema.");
//						TODO: jtpaulo - Qual era o problema c/ a leitura da chave e do configuration file.
						this.buildDatabaseSchema(file.getPath());
						parsingComplete = true;
					}

				} finally {
					lock.unlock();
				}
			}

			if(databaseSchema.containsKey(tableName)) {
				this.tableSchema = getTableSchema(tableName);
			}
			else {
				this.tableSchema = generateDefaultTableSchema(tableName, conf);
				LOG.debug("Generate Default Table Schema for "+tableName+" table: "+tableSchema.toString());
			}

			this.cryptoProperties = new CryptoProperties(this.tableSchema);

		} else {
			throw new FileNotFoundException("Schema file not found.");
		}
	}

//	TODO: esta parte depois tem de ser feita com um gestor de chaves
//	FIXME: The current approach instantiate the same cryptograhic key for all CryptoBox. Solution: Provide different cryptographic keys for each CryptoBox
	private void setCryptographicKey(Configuration conf) throws FileNotFoundException {
		String cryptographicKeyProperty = conf.get("cryptographickey");
		if(cryptographicKeyProperty != null && !cryptographicKeyProperty.isEmpty()) {
			while (!keyAcknowledgement) {
				try {
					lock.lock();
					if (!keyAcknowledgement) {
						File keyFile = new File(cryptographicKeyProperty);

						if (keyFile.isFile() && keyFile.getName().equals("key.txt")) {
							LOG.debug("File key.");
							cryptographicKey = Utils.readKeyFromFile(keyFile.getPath());
						} else {
							throw new FileNotFoundException("The file "+cryptographicKeyProperty+" does not match with the requirements.");
						}
						keyAcknowledgement = true;
					}
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					lock.unlock();
				}

			}
		} else {
			throw new FileNotFoundException("Cryptographic Key file not found.");
		}

	}

	/**
	 * init(String filename) method : read and creates a database secure schema
	 * @param filename path to the database schema
	 * @return TableSchema object
	 */
	private void buildDatabaseSchema(String filename) {
		if (filename == null) {
			throw new NullPointerException("Schema file name cannot be null.");
		}

		SchemaParser schemaParser = new SchemaParser();
		schemaParser.parseDatabaseTables(filename);

		databaseSchema = new HashMap<>();
		databaseSchema = schemaParser.getSchemas();

		databaseDefaultProperties = new HashMap<>();
		databaseDefaultProperties = schemaParser.getDatabaseDefaultProperties();

	}

	private TableSchema getTableSchema(String tablename) {
		return databaseSchema.get(tablename);
	}

	private TableSchema generateDefaultTableSchema(String tablename, Configuration conf) {
		TableSchema temp = new TableSchema();
		temp.setTablename(tablename);
		temp.setDefaultKeyCryptoType((CryptoTechnique.CryptoType) databaseDefaultProperties.get("defaultPropertiesKey"));
		temp.setDefaultColumnsCryptoType((CryptoTechnique.CryptoType) databaseDefaultProperties.get("defaultPropertiesColumns"));
		temp.setDefaultKeyFormatSize((Integer) databaseDefaultProperties.get("defaultPropertiesKeyFormatSize"));
		temp.setDefaultColumnFormatSize((Integer) databaseDefaultProperties.get("defaultPropertiesColFormatSize"));
		temp.setDefaultKeyPadding((Boolean) databaseDefaultProperties.get("defaultPropertiesKeyPadding"));
		temp.setDefaultColumnPadding((Boolean) databaseDefaultProperties.get("defaultPropertiesColumnPadding"));
		temp.setEncryptionMode((Boolean) databaseDefaultProperties.get("defaultPropertiesEncryptionMode"));
//		KEY
//		FIXME: does not contemplate FPE
		temp.setKey(
				new Key(
						(CryptoTechnique.CryptoType) databaseDefaultProperties.get("defaultPropertiesKey"),
						(Integer) databaseDefaultProperties.get("defaultPropertiesKeyFormatSize"),
						(Boolean) databaseDefaultProperties.get("defaultPropertiesKeyPadding")));


//		FIXME: does not contemplate FPE
//		COLUMN FAMILIES
		HTableDescriptor descriptor = null;
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if(admin.tableExists(tablename)) {
				descriptor = admin.getTableDescriptor(TableName.valueOf(tablename));
			}

			HColumnDescriptor[] columnDescriptors;
			if(descriptor != null) {
				columnDescriptors = descriptor.getColumnFamilies();
			}
			else {
				columnDescriptors = null;
			}

			if(columnDescriptors != null) {
				for(HColumnDescriptor hcol :columnDescriptors) {
					Family tempFamily = new Family(
							hcol.getNameAsString(),
							(CryptoTechnique.CryptoType) databaseDefaultProperties.get("defaultPropertiesColumns"),
							(Integer) databaseDefaultProperties.get("defaultPropertiesColFormatSize"),
							(Boolean) databaseDefaultProperties.get("defaultPropertiesColumnPadding"));
					temp.addFamily(tempFamily);
				}
			}

		} catch (Exception e) {
			LOG.error("CryptoTable:generateDefaultTableSchema:Exception:"+e.getMessage());
		}

		return temp;
	}


//----------------------------------------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------------------------------------

	/**
	 * put(Put put) method : secure put/update method.
	 * The original put object sends a set of qualifiers and values to insert in the database system.
	 * Before the insertion both key and values are encrypted, following the database schema specifications.
	 * In case of OPE CryptoBox, an additional qualifier is created and stores the respective value encrypted with the STD CryptoBox
	 * @param put original put object that contains the key, values, qualifiers, ...
	//	 */
	@Override
	public void put(Put put) {
		Put finalPut;
		try {
			if (this.tableSchema.getEncryptionMode()) {
				finalPut = this.encodePutObject(put);
			} else {
				finalPut = put;
			}

			super.put(finalPut);

		} catch (Exception e) {
			LOG.error("Exception in put method. " + e.getMessage());
		}
	}

	@Override
	public void put(List<Put> puts) {
		List<Put> finalPutList;
		try {
			if (this.tableSchema.getEncryptionMode()) {
				finalPutList = new ArrayList<>(puts.size());
				for(Put p : puts) {
					finalPutList.add(this.encodePutObject(p));
				}
			} else {
				finalPutList = puts;
			}

			super.put(finalPutList);

		} catch (Exception e) {
			LOG.error("Exception in put (batch) method. " + e.getMessage());
		}
	}

	/**
	 * get(Get get) method : secure get method.
	 * The original get object sets the Row-Key to search in the database system. Before the get operation, the Row-Key
	 * is encrypted accordingly the respective CryptoBox and its issued. After the server response, all the values must be
	 * decoded with the respective CryptoBox, resulting in the original values stored by the user.
	 * @param get original get object that contains the key to perform the operation.
	 * @return Result containing the plaintext value of the get result.
	 */
	@Override
	public Result get(Get get) {
		Result getResult = Result.EMPTY_RESULT;
		try {
			if (this.tableSchema.getEncryptionMode()) {
				byte[] row = this.getObjectRow(get);

				Map<byte[], List<byte[]>> columns = this.cryptoProperties.getHColumnDescriptors(get.getFamilyMap());

				Object encodedGet = this.encodeGet(row, columns);

				switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
					case STD:
						getResult = this.decodeGetObject((ResultScanner) encodedGet, row);
						break;
					case PLT:
					case DET:
					case OPE:
					case FPE:
						Result encryptedResult = super.get((Get) encodedGet);
						getResult = this.decodeGetObject(encryptedResult, row);
						break;
					default:
						break;
				}

			} else {
				getResult = super.get(get);
			}
		} catch (Exception e) {
			LOG.error("Exception in get method. " + e.getMessage());
		}

		return getResult;
	}

	@Override
	public Result[] get(List<Get> gets) {
		Result[] results = new Result[gets.size()];
		try {
			if(this.tableSchema.getEncryptionMode()) {
				List<Get> encryptedGets = new ArrayList<>(gets.size());

				for (Get g : gets) {
					byte[] row = this.getObjectRow(g);
					Map<byte[], List<byte[]>> columns = this.cryptoProperties.getHColumnDescriptors(g.getFamilyMap());

//					First phase: Encrypt all get objects
					switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
						case PLT:
						case DET:
						case OPE:
						case FPE:
							Get encGet = new Get(this.cryptoProperties.encodeRow(row));
							this.wrapHColumnDescriptors(encGet, columns);
							encryptedGets.add(encGet);

							break;
						case STD:
						default:
							break;
					}
				}

//				Second phase: call super() to batch the Get's List
				switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
					case STD:
						ResultScanner encryptedScan = super.getScanner(new Scan());
						int noMoreGetObjects = 0;

						for (Result r = encryptedScan.next(); r != null; r = encryptedScan.next()) {
							if (noMoreGetObjects != gets.size()) {
								byte[] resultRow = this.cryptoProperties.decodeRow(r.getRow());
								for (Get get : gets) {
									if (Arrays.equals(resultRow, this.getObjectRow(get))) {
										results[gets.indexOf(get)] = this.cryptoProperties.decodeResult(resultRow, r);
										noMoreGetObjects++;
										break;
									}
								}
							} else {
								break;
							}
						}

						break;
					case PLT:
					case DET:
					case OPE:
					case FPE:
						Result[] encryptedResults = super.get(encryptedGets);
						for (int i = 0; i < encryptedResults.length; i++) {
	//						Third phase: decode result
							results[i] = this.decodeGetObject(encryptedResults[i]);
						}
						break;
					default:
						break;
				}
			} else {
				results = super.get(gets);
			}
		} catch (Exception e) {
			LOG.error("Exception in get method. " + e.getMessage());
		}

		return results;
	}

	/**
	 * delete(Delete delete) method : secure delete method.
	 * The original delete object sets the Row-Key to search in the database system. Before the delete operation, the Row-Key
	 * is encrypted accordingly the respective CryptoBox and its issued.
	 * @param delete original get object that contains the key to perform the operation.
	 */
	@Override
	public void delete(Delete delete) {
		try {
			if (this.tableSchema.getEncryptionMode()) {
				byte[] row = this.getObjectRow(delete);
				List<String> hcolumnsToDelete = this.htableUtils.deleteCells(delete.cellScanner());

//				Verify the Row-Key CryptoBox
				switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
					case STD:
						ResultScanner encScan = super.getScanner(new Scan());
						for (Result r = encScan.next(); r != null; r = encScan.next()) {
							byte[] resultRowKey = this.cryptoProperties.decodeRow(r.getRow());
							if (Arrays.equals(row, resultRowKey)) {
								super.delete(this.encodeDeleteObject(row, hcolumnsToDelete));
								break;
							}
						}
						break;
					case PLT:
					case DET:
					case OPE:
					case FPE:
						super.delete(this.encodeDeleteObject(row, hcolumnsToDelete));
						break;
					default:
						break;
				}
			} else {
				super.delete(delete);
			}
		} catch (Exception e) {
			LOG.error("Exception in delete method. " + e.getMessage());
		}
	}

	@Override
	public void delete(List<Delete> deletes) {
		try {
			if (this.tableSchema.getEncryptionMode()) {
				List<Delete> encryptedDeletes = new ArrayList<>(deletes.size());
				CryptoTechnique.CryptoType cType = this.cryptoProperties.tableSchema.getKey().getCryptoType();

				if (cType == CryptoTechnique.CryptoType.STD) {
					int noMoreDeleteObjects = 0;
					ResultScanner encryptedScanner = super.getScanner(new Scan());

					for (Result r = encryptedScanner.next(); r != null; r = encryptedScanner.next()) {
						if (noMoreDeleteObjects != deletes.size()) {
							byte[] resultRow = this.cryptoProperties.decodeRow(r.getRow());
							for (Delete delete : deletes) {
								if (Arrays.equals(resultRow, this.getObjectRow(delete))) {
									encryptedDeletes.add(this.encodeDeleteObject(delete.getRow(), this.htableUtils.deleteCells(delete.cellScanner())));
									noMoreDeleteObjects++;
									break;
								}
							}
						} else {
							break;
						}
					}
				} else {
					for (Delete del : deletes) {
						byte[] row = this.getObjectRow(del);
						List<String> hcolumnsToDelete = this.htableUtils.deleteCells(del.cellScanner());
						encryptedDeletes.add(this.encodeDeleteObject(row, hcolumnsToDelete));
					}
				}

				super.delete(encryptedDeletes);
			} else {
				super.delete(deletes);
			}

		} catch(Exception e) {
			LOG.error("Exception in delete (list<Delete> deletes) method. " + e.getMessage());
		}
	}

	/**
	 * getScanner(Scan scan) method : secure scan and filter operations
	 * This operations provides the secure scan and filter operations over the database. Encrypting both start row, stop row
	 * and compare value.
	 * @param scan scan object that provides the necessary filter and scan parameters.
	 * @return resulting values that pass the filter parameters. The values are still encrypted.
	//	 */
	@Override
	public ResultScanner getScanner(Scan scan) {
		ResultScanner result = null;
		try {
			if (this.tableSchema.getEncryptionMode()) {
				byte[] startRow = scan.getStartRow();
				byte[] endRow = scan.getStopRow();

//				Transform the original object in an encrypted scan.
				Scan encScan = this.htableUtils.buildEncryptedScan(scan);
				encScan.setCaching(scan.getCaching());

				ResultScanner encryptedResultScanner = super.getScanner(encScan);
//				Return the corresponding result scanner to decrypt the resulting set of values
				result = this.resultScannerFactory.getResultScanner(
						this.htableUtils.verifyFilterCryptoType(scan),
						this.cryptoProperties,
						startRow,
						endRow,
						encryptedResultScanner,
						this.htableUtils.parseFilter(scan.getFilter()));

			} else {
				result = super.getScanner(scan);
			}
		} catch (Exception e) {
			LOG.error("Exception in scan method. " + e.getMessage());
		}
		return result;
	}

	@Override
	public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) {
		boolean operationPerformed = false;

		try {
			if (this.tableSchema.getEncryptionMode()) {
				this.verifyNullableByteArray(row);
				this.verifyNullableByteArray(family);
				this.verifyNullableByteArray(qualifier);
				this.verifyNullableByteArray(value);

				switch (this.tableSchema.getKey().getCryptoType()) {
					case STD:
//						step 1 : get all stored values
						ResultScanner rs = super.getScanner(new Scan());
//						step 2 : check if specified row exists
						for (Result r = rs.next(); r != null; r = rs.next()) {
							byte[] resultRow = this.cryptoProperties.decodeRow(r.getRow());
							if (Arrays.equals(row, resultRow)) {
//								Get the stored value for the specified family and qualifier and check if it's equal to a given value
								byte[] encryptedValue = r.getValue(family, qualifier);
								if (encryptedValue != null && encryptedValue.length > 0) {
									byte[] resultValue = this.cryptoProperties.decodeValue(family, qualifier, encryptedValue);
									if (Arrays.equals(value, resultValue)) {
//										If the values match, build and encrypted put
										Put encryptedPut;
										if (!put.isEmpty()) {
											encryptedPut = new Put(r.getRow());
											this.htableUtils.encryptCells(put.cellScanner(), this.tableSchema, encryptedPut, this.cryptoProperties);
										} else {
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
					case PLT:
					case DET:
					case OPE:
					case FPE:
//						step 1 : encrypt row and value
						byte[] encryptedRow = this.cryptoProperties.encodeRow(row);
						byte[] encryptedValue;
						if (cryptoProperties.tableSchema.getCryptoTypeFromQualifier(new String(family), new String(qualifier)) == CryptoTechnique.CryptoType.STD) {
							Result encryptedResult = super.get(new Get(encryptedRow));
							Result temp_result = this.cryptoProperties.decodeResult(row, encryptedResult);
							byte[] temp_val = temp_result.getValue(family, qualifier);

							if (Arrays.equals(temp_val, value)) {
								encryptedValue = encryptedResult.getValue(family, qualifier);
							} else {
								throw new NullPointerException("Specified value does not match with the stored version.");
							}
						} else {
							encryptedValue = this.cryptoProperties.encodeValue(family, qualifier, value);
						}

//						step 2 : encrypt put
						Put encryptedPut;
						if (!put.isEmpty()) {
							encryptedPut = new Put(this.cryptoProperties.encodeRow(put.getRow()));
							this.htableUtils.encryptCells(put.cellScanner(), this.tableSchema, encryptedPut, this.cryptoProperties);
						} else {
							throw new NullPointerException("Put object cannot be null");
						}
//						step 3 : call super
						operationPerformed = super.checkAndPut(encryptedRow, family, qualifier, encryptedValue, encryptedPut);
						break;
					default:
						break;
				}
			} else {
				operationPerformed = super.checkAndPut(row, family, qualifier, value, put);
			}
		} catch (Exception e) {
			LOG.error("Exception in checkAndPut method. "+e.getMessage());
		}
		return operationPerformed;
	}

	@Override
	public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) {
		long operationValue = 0;
		try {
			if (this.tableSchema.getEncryptionMode()) {
				this.verifyNullableByteArray(row);
				this.verifyNullableByteArray(family);
				this.verifyNullableByteArray(qualifier);

				String temp_family = new String(family);
				String temp_qualifier = new String(qualifier);

				switch (this.tableSchema.getCryptoTypeFromQualifier(temp_family, temp_qualifier)) {
					case PLT:
						if (this.cryptoProperties.tableSchema.getKey().getCryptoType() != CryptoTechnique.CryptoType.STD) {
							operationValue = super.incrementColumnValue(this.cryptoProperties.encodeRow(row), family, qualifier, amount);
						} else {
							ResultScanner stdScanner = super.getScanner(new Scan());
							for (Result r = stdScanner.next(); r != null; r = stdScanner.next()) {
								byte[] aux = this.cryptoProperties.decodeRow(r.getRow());
								if (Arrays.equals(row, aux)) {
									operationValue = super.incrementColumnValue(r.getRow(), family, qualifier, amount);
									break;
								}
							}
						}
						break;
					case STD:
					case DET:
					case OPE:
					case FPE:
						throw new UnsupportedOperationException("Secure operation not supported. Only for vanilla instance.");
					default:
						break;
				}
			} else {
				operationValue = super.incrementColumnValue(row, family, qualifier, amount);
			}
		} catch (IOException e) {
			LOG.error("Exception in incrementColumnValue method. "+e.getMessage());
		}
		return operationValue;
	}

	@Override
	public HRegionLocation getRegionLocation(byte[] row) {
		HRegionLocation hRegionLocation = null;
		try {
			if (this.tableSchema.getEncryptionMode()) {
				if (row.length == 0) {
					throw new NullPointerException("Row-Key cannot be null.");
				}

				switch (this.tableSchema.getKey().getCryptoType()) {
					case STD:
						HRegionLocation stdHRegionLocation = null;
						ResultScanner rs = super.getScanner(new Scan());
						for (Result r = rs.next(); r != null; r = rs.next()) {
							if (!r.isEmpty()) {
								byte[] temp_row = this.cryptoProperties.decodeRow(r.getRow());
								if (Arrays.equals(temp_row, row)) {
									stdHRegionLocation = super.getRegionLocation(r.getRow());
									break;
								}
							}
						}
						hRegionLocation = stdHRegionLocation;
						break;
					case PLT:
					case DET:
					case OPE:
					case FPE:
						hRegionLocation = super.getRegionLocation(this.cryptoProperties.encodeRow(row));
						break;
					default:
						break;
				}
			} else {
				hRegionLocation = super.getRegionLocation(row);
			}
		} catch (Exception e) {
			LOG.error("Exception in getRegionLocation method. "+e.getMessage());
		}
		return hRegionLocation;
	}

	@Override
	public Result getRowOrBefore(byte[] row, byte[] family) {
		Result result = Result.EMPTY_RESULT;
		try {
			if (this.tableSchema.getEncryptionMode()) {
				this.verifyNullableByteArray(row);
				this.verifyNullableByteArray(family);

				switch (this.tableSchema.getKey().getCryptoType()) {
					case STD:
					case DET:
					case FPE:
						ResultScanner rs = super.getScanner(new Scan());
						byte[] encryptedRowBefore = null;
						byte[] plaintextRowBefore = null;
						byte[] decodedRow;
						Result encResult = null;
						Bytes.ByteArrayComparator bcomp = new Bytes.ByteArrayComparator();
						for (Result r = rs.next(); r != null; r = rs.next()) {
							decodedRow = this.cryptoProperties.decodeRow(r.getRow());
							if (Arrays.equals(decodedRow, row)) {
								encResult = super.getRowOrBefore(r.getRow(), family);
								break;
							} else {
								if (plaintextRowBefore == null) {
									encryptedRowBefore = r.getRow();
									plaintextRowBefore = decodedRow.clone();
								} else if (bcomp.compare(row, decodedRow) > 0 && bcomp.compare(plaintextRowBefore, decodedRow) < 0) {
									plaintextRowBefore = decodedRow.clone();
									encryptedRowBefore = r.getRow();
								}
							}
						}

						if (encResult == null) {
							encResult = super.getRowOrBefore(encryptedRowBefore, family);
						}

						result = this.cryptoProperties.decodeResult(this.cryptoProperties.decodeRow(encResult.getRow()), encResult);
						break;
					case PLT:
					case OPE:
						Result encryptedResult = super.getRowOrBefore(this.cryptoProperties.encodeRow(row), family);
						result = this.cryptoProperties.decodeResult(this.cryptoProperties.decodeRow(encryptedResult.getRow()), encryptedResult);
						break;
					default:
						break;
				}
			} else {
				result = super.getRowOrBefore(row, family);
			}
		} catch (Exception e) {
			LOG.error("Exception in getRowOrBefore method. " + e.getMessage());
		}
		return result;
	}

//----------------------------------------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------------------------------------

	private Put encodePutObject(Put p) {
		byte[] row = this.getObjectRow(p);

//		Encode the Row-Key
		Put encryptedObject = new Put(this.cryptoProperties.encodeRow(row));
		this.htableUtils.encryptCells(p.cellScanner(), this.tableSchema, encryptedObject, this.cryptoProperties);
		return encryptedObject;
	}

	private byte[] getObjectRow(Row object) {
		byte[] row = object.getRow();
		this.verifyNullableByteArray(row);

		return row;
	}

	private void verifyNullableByteArray(byte[] content) {
		if (content.length == 0) {
			throw new NullPointerException("Byte[] cannot be null.");
		}
	}

	private Object encodeGet(byte[] row,  Map<byte[], List<byte[]>> columns) {
		Object result = null;
		try {

			switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
				case STD:
					Scan stdGetScan = new Scan();
					this.wrapHColumnDescriptors(stdGetScan, columns);

					result = super.getScanner(stdGetScan);

					break;
				case PLT:
				case DET:
				case OPE:
				case FPE:
					Get encGet = new Get(this.cryptoProperties.encodeRow(row));
					this.wrapHColumnDescriptors(encGet, columns);

					result = encGet;

					break;
				default:
					break;
			}

		} catch (Exception e) {
			LOG.error("Exception in get method. " + e.getMessage());
		}

		return result;
	}

	private Result decodeGetObject(Result object) {
		Result getResult = Result.EMPTY_RESULT;
		try {
			if (object != null) {
				if (!object.isEmpty()) {
					byte[] row = this.cryptoProperties.decodeRow(object.getRow());
					getResult = this.cryptoProperties.decodeResult(row, object);
				}
			} else {
				throw new NullPointerException("decodeGetObject: object cannot be null");
			}
		} catch (Exception e) {
			LOG.error("Exception in decodeGetObject method. " + e.getMessage());
		}

		return getResult;
	}

	private Result decodeGetObject(Result object, byte[] row) {
		Result getResult = Result.EMPTY_RESULT;
		try {
			if (object != null) {
				if (!object.isEmpty()) {
					getResult = this.cryptoProperties.decodeResult(row, object);
				}
			} else {
				throw new NullPointerException("decodeGetObject: object cannot be null");
			}
		} catch (Exception e) {
			LOG.error("Exception in decodeGetObject method. " + e.getMessage());
		}

		return getResult;
	}

	private Result decodeGetObject(ResultScanner object, byte[] row) {
		Result getResult = Result.EMPTY_RESULT;
		try {
			if (object != null) {
				for (Result r = object.next(); r != null; r = object.next()) {
					byte[] aux = this.cryptoProperties.decodeRow(r.getRow());
					if (Arrays.equals(row, aux)) {
						getResult = this.cryptoProperties.decodeResult(row, r);
						break;
					}
				}
			} else {
				throw new NullPointerException("decodeGetObject: Unrecognized object type.");
			}
		} catch (Exception e) {
			LOG.error("Exception in decodeGetObject method. " + e.getMessage());
		}

		return getResult;
	}

	private void wrapHColumnDescriptors(Get object, Map<byte[], List<byte[]>> columns) {
		for (byte[] f : columns.keySet()) {
			for (byte[] q : columns.get(f)) {
				object.addColumn(f, q);
			}
		}
	}

	private void wrapHColumnDescriptors(Scan object, Map<byte[], List<byte[]>> columns) {
		for (byte[] f : columns.keySet()) {
			for (byte[] q : columns.get(f)) {
				object.addColumn(f, q);
			}
		}
	}

	private Delete encodeDeleteObject(byte[] row, List<String> hcolumnsToDelete) {
		Delete encryptedObject = new Delete(this.cryptoProperties.encodeRow(row));
		this.htableUtils.wrapDeletedCells(hcolumnsToDelete, encryptedObject);
		return encryptedObject;
	}

}

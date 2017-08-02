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
	public TableSchema tableSchema;


	public CryptoTable(Configuration conf, String tableName) throws IOException {
		super(conf, TableName.valueOf(tableName));
		String property = conf.get("schema");
		LOG.debug("CryptoTable:TableName:"+tableName);

		if(property != null && !property.isEmpty()) {
			File file = new File(property);

			while(!parsingComplete) {
				try {
					lock.lock();
					System.out.println("Thread-"+Thread.currentThread().getId() + ":Lock achieved.");
					if (!parsingComplete) {
						System.out.println("Thread-"+Thread.currentThread().getId() + ":build database schema.");

						this.buildDatabaseSchema(file.getPath(), conf);
						parsingComplete = true;
					}

				} finally {
					lock.unlock();
					System.out.println("Thread-"+Thread.currentThread().getId() + ":Lock released.");
					System.out.println("Thread-"+Thread.currentThread().getId() + ":Shared ObjectId (database schema):" + System.identityHashCode(databaseSchema));
				}
			}

//			FIXME: a tabela pode nao existir no table schema. Se nao existir Cria-se um table schema a default
			if(databaseSchema.containsKey(tableName)) {
				this.tableSchema = getTableSchema(tableName);
				this.cryptoProperties = new CryptoProperties(this.tableSchema);
			}
			else {
				System.out.println("Tablename not found in schema file.");
				this.tableSchema = generateDefaultTableSchema(tableName, conf);
				this.cryptoProperties = new CryptoProperties(this.tableSchema);
				System.out.println("Generate Default Table Schema for "+tableName+" table: "+tableSchema.toString());
			}
		} else {
			throw new FileNotFoundException("Schema file not found.");
		}


//		While the cryptographic keys management is not defined, read keys from specific files
//		arrange cryptographic keys properties
//		TODO: esta parte depois tem de ser feita com um gestor de chaves
//		TODO - Apply locks here
		File cryptographicKey = new File("src/main/resources/key.txt");
		byte[] key;
		if(cryptographicKey.isFile() && cryptographicKey.getName().equals("key.txt")) {
			LOG.debug("File key available.");
			key = Utils.readKeyFromFile(cryptographicKey.getPath());
		}
		else {
			LOG.debug("No key available. Default key used.");
			key = new byte[]{(byte) 0x2B, (byte) 0x7E, (byte) 0x15, (byte) 0x16, (byte) 0x28, (byte) 0xAE, (byte) 0xD2,
					(byte) 0xA6, (byte) 0xAB, (byte) 0xF7, (byte) 0x15, (byte) 0x88, (byte) 0x09, (byte) 0xCF,
					(byte) 0x4F, (byte) 0x3C};
		}

		for(CryptoTechnique.CryptoType cryptoType : this.tableSchema.getEnabledCryptoTypes()) {
			this.cryptoProperties.setKey(cryptoType, key);
		}

//		this is common for both cases
		this.resultScannerFactory = new ResultScannerFactory();
		this.secureFilterConverter = new SecureFilterConverter(this.cryptoProperties);
		this.htableUtils = new HTableFeaturesUtils(this.cryptoProperties, this.secureFilterConverter);
	}

	/**
	 * init(String filename) method : read and creates a database secure schema
	 * @param filename path to the database schema
	 * @return TableSchema object
	 */
	public void buildDatabaseSchema(String filename, Configuration conf) {
		if (filename == null) {
			throw new NullPointerException("Schema file name cannot be null.");
		}

		SchemaParser schemaParser = new SchemaParser(conf);
		schemaParser.parseDatabaseTables(filename);

		databaseSchema = new HashMap<>();
		databaseSchema = schemaParser.getSchemas();

		databaseDefaultProperties = new HashMap<>();
		databaseDefaultProperties = schemaParser.getDatabaseDefaultProperties();

	}

	public TableSchema getTableSchema(String tablename) {
		return databaseSchema.get(tablename);
	}

	public TableSchema generateDefaultTableSchema(String tablename, Configuration conf) {
		TableSchema temp = new TableSchema();
		temp.setTablename(tablename);
		temp.setDefaultKeyCryptoType((CryptoTechnique.CryptoType) databaseDefaultProperties.get("defaultPropertiesKey"));
		temp.setDefaultColumnsCryptoType((CryptoTechnique.CryptoType) databaseDefaultProperties.get("defaultPropertiesColumns"));
		temp.setDefaultKeyFormatSize((Integer) databaseDefaultProperties.get("defaultPropertiesKeyFormatSize"));
		temp.setDefaultColumnFormatSize((Integer) databaseDefaultProperties.get("defaultPropertiesColFormatSize"));
		temp.setDefaultKeyPadding((Boolean) databaseDefaultProperties.get("defaultPropertiesKeyPadding"));
		temp.setDefaultColumnPadding((Boolean) databaseDefaultProperties.get("defaultPropertiesColumnPadding"));

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

	/**
	 * put(Put put) method : secure put/update method.
	 * The original put object sends a set of qualifiers and values to insert in the database system.
	 * Before the insertion both key and values are encrypted, following the database schema specifications.
	 * In case of OPE CryptoBox, an additional qualifier is created and stores the respective value encrypted with the STD CryptoBox
	 * @param put original put object that contains the key, values, qualifiers, ...
//	 */
	@Override
	public void put(Put put) {
		LOG.debug("Put operation started.");
		try {
			byte[] row = put.getRow();
			if(row.length == 0) {
				throw new NullPointerException("Row Key cannot be null.");
			}

//			Encode the row key
			Put encPut = new Put(this.cryptoProperties.encodeRow(row));
			this.htableUtils.encryptCells(put.cellScanner(), this.tableSchema, encPut, this.cryptoProperties);
			super.put(encPut);

		} catch (Exception e) {
			LOG.error("Exception in put method. " + e.getMessage());
		}
	}

//	@Override
//	public void put(Put put) {
//		try {
//			LOG.debug("SUPER:PUT: "+put.toString());
//			super.put(put);
//		} catch (InterruptedIOException e) {
//			e.printStackTrace();
//		} catch (RetriesExhaustedWithDetailsException e) {
//			e.printStackTrace();
//		}
//	}

	@Override
	public void put(List<Put> puts) {
		LOG.debug("Batch Put operation started.");
		try {
			List<Put> encryptedPuts = new ArrayList<>(puts.size());
			for(Put p : puts) {
				byte[] row = p.getRow();
				if(row.length == 0) {
					throw new NullPointerException("Row Key cannot be null.");
				}

//				Encode the row key
				Put encPut = new Put(this.cryptoProperties.encodeRow(row));
				this.htableUtils.encryptCells(p.cellScanner(), this.tableSchema, encPut, this.cryptoProperties);
				encryptedPuts.add(encPut);
			}

			super.put(encryptedPuts);

		} catch (InterruptedIOException | RetriesExhaustedWithDetailsException e) {
			LOG.error("Exception in put (list<Put> puts) method. " + e.getMessage());
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
		LOG.debug("Get operation started.");

		Result getResult = Result.EMPTY_RESULT;

		try {
			byte[] row = get.getRow();
			if(row.length == 0) {
				throw new NullPointerException("Row Key cannot be null.");
			}

			Map<byte[],List<byte[]>> columns = this.cryptoProperties.getHColumnDescriptors(get.getFamilyMap());

//			Verify the row key CryptoBox
			switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
				case STD :
					Scan stdGetScan = new Scan();
					for(byte[] f : columns.keySet()) {
						for(byte[] q : columns.get(f)){
							stdGetScan.addColumn(f, q);
						}
					}

					ResultScanner encScan = super.getScanner(stdGetScan);
					for (Result r = encScan.next(); r != null; r = encScan.next()) {
						byte[] aux = this.cryptoProperties.decodeRow(r.getRow());
						row = Utils.removePadding(row);

						if (Arrays.equals(row, aux)) {
							getResult = this.cryptoProperties.decodeResult(row, r);
							break;
						}
					}
					return getResult;
				case PLT :
				case DET :
				case OPE :
				case FPE :
					Get encGet = new Get(this.cryptoProperties.encodeRow(row));

					for(byte[] f : columns.keySet()) {
						for(byte[] q : columns.get(f)){
							encGet.addColumn(f, q);
						}
					}

					Result res = super.get(encGet);

					if (!res.isEmpty()) {
						getResult = this.cryptoProperties.decodeResult(row, res);
						LOG.debug("Get:Result:"+getResult.toString());
					}
					else {
						LOG.debug("Get:EmptyResult");
					}

					return getResult;
				default :
					break;
			}

		} catch (IOException e) {
			LOG.error("Exception in get method. " + e.getMessage());
		}
		return getResult;
	}



	@Override
	public Result[] get(List<Get> gets) {
		LOG.debug("Batch Get operation started.");
		Result[] results = new Result[gets.size()];
		List<Get> encryptedGets = new ArrayList<>(gets.size());

		for(Get g : gets) {
			byte[] row = g.getRow();
			if(row.length == 0) {
				throw new NullPointerException("Row Key cannot be null.");
			}

			Map<byte[],List<byte[]>> columns = this.cryptoProperties.getHColumnDescriptors(g.getFamilyMap());


//			First phase: Encrypt all get objects
			switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
				case PLT :
				case DET :
				case OPE :
				case FPE :
					Get encGet = new Get(this.cryptoProperties.encodeRow(row));

					for(byte[] f : columns.keySet()) {
						for(byte[] q : columns.get(f)){
							encGet.addColumn(f, q);
						}
					}

					encryptedGets.add(encGet);
					break;
				case STD :
					for(byte[] f : columns.keySet()) {
						for(byte[] q : columns.get(f)){
							g.addColumn(f, q);
						}
					}

					encryptedGets.add(g);
					break;
				default :
					break;
			}
		}

//		Second phase: call super() to batch the Get's List
		try {
			switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
				case STD :
					for(int i = 0; i < encryptedGets.size(); i++) {
						byte[] row = encryptedGets.get(i).getRow();
						ResultScanner encScan = super.getScanner(new Scan());
						for (Result r = encScan.next(); r != null; r = encScan.next()) {
							byte[] aux = this.cryptoProperties.decodeRow(r.getRow());
							row = Utils.removePadding(row);

//							Third phase: decode result
							if (Arrays.equals(row, aux)) {
								results[i] = this.cryptoProperties.decodeResult(row, r);
								break;
							}
						}

					}
					break;
				case PLT:
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

	/**
	 * delete(Delete delete) method : secure delete method.
	 * The original delete object sets the row key to search in the database system. Before the delete operation, the row key
	 * is encrypted accordingly the respective CryptoBox and its issued.
	 * @param delete original get object that contains the key to perform the operation.
	 */
	@Override
	public void delete(Delete delete) {
		LOG.debug("Delete operation started.");
		try {
			byte[] row = delete.getRow();
			if(row.length == 0) {
				throw new NullPointerException("Row Key cannot be null.");
			}

			List<String> cellsToDelete = this.htableUtils.deleteCells(delete.cellScanner());

//			Verify the row key CryptoBox
			switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
				case STD :
					ResultScanner encScan = super.getScanner(new Scan());
					for (Result r = encScan.next(); r != null; r = encScan.next()) {
						byte[] resultRowKey = this.cryptoProperties.decodeRow(r.getRow());
						row = Utils.removePadding(row);

						if (Arrays.equals(row, resultRowKey)) {
							Delete del = new Delete(r.getRow());
							this.htableUtils.wrapDeletedCells(cellsToDelete, del);
							super.delete(del);
							break;
						}
					}
					break;
				case PLT:
				case DET :
				case OPE :
				case FPE :
					Delete encDelete = new Delete(this.cryptoProperties.encodeRow(row));
					this.htableUtils.wrapDeletedCells(cellsToDelete, encDelete);
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
	public void delete(List<Delete> deletes) {
		LOG.debug("Batch Delete operation started.");
		try {
			List<Delete> encryptedDeletes = new ArrayList<>(deletes.size());
			for (Delete del : deletes) {
				byte[] row = del.getRow();
				if (row.length == 0) {
					throw new NullPointerException("Row Key cannot be null.");
				}

				List<String> cellsToDelete = this.htableUtils.deleteCells(del.cellScanner());

//				Verify the row key CryptoBox
				switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
					case STD:
						ResultScanner encScan = super.getScanner(new Scan());
						for (Result r = encScan.next(); r != null; r = encScan.next()) {
							byte[] resultValue = this.cryptoProperties.decodeRow(r.getRow());
							row = Utils.removePadding(row);

							if (Arrays.equals(row, resultValue)) {
								Delete encryptedDelete = new Delete(r.getRow());
								this.htableUtils.wrapDeletedCells(cellsToDelete, encryptedDelete);
								encryptedDeletes.add(encryptedDelete);
								break;
							}
						}
						break;
					case PLT:
					case DET:
					case OPE:
					case FPE:
						Delete encryptedDelete = new Delete(this.cryptoProperties.encodeRow(row));
						this.htableUtils.wrapDeletedCells(cellsToDelete, encryptedDelete);
						encryptedDeletes.add(encryptedDelete);
						break;
					default:
						break;
				}
			}

			super.delete(encryptedDeletes);

		} catch(IOException e) {
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
		LOG.debug("GetScanner operation started.");
		try {
			byte[] startRow = scan.getStartRow();
			byte[] endRow = scan.getStopRow();

//			Transform the original object in an encrypted scan.
			LOG.debug("SCAN:Vanilla:"+scan.toString());
			Scan encScan = this.htableUtils.buildEncryptedScan(scan);
			encScan.setCaching(scan.getCaching());

			ResultScanner encryptedResultScanner = super.getScanner(encScan);
//			Return the corresponding result scanner to decrypt the resulting set of values
			ResultScanner rs = this.resultScannerFactory.getResultScanner(
					this.htableUtils.verifyFilterCryptoType(scan),
					this.cryptoProperties,
					startRow,
					endRow,
					encryptedResultScanner,
					this.htableUtils.parseFilter(scan.getFilter()));

			LOG.debug("SCAN:ResultScanner:ClassType:"+rs.getClass().getSimpleName());

			return rs;
		} catch (Exception e) {
			LOG.error("Exception in scan method. " + e.getMessage());
		}
		return null;
	}


	@Override
	public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) {
		LOG.debug("CheckAndPut operation started.");
		boolean operationPerformed = false;

		try {
			if(row.length == 0) {
				throw new NullPointerException("Row Key cannot be null.");
			}
			if(family == null) {
				throw new NullPointerException("Column family cannot be null.");
			}
			if(qualifier == null) {
				throw new NullPointerException("Column qualifier cannot be null.");
			}
			if(value == null) {
				throw new NullPointerException("Value cannot be null.");
			}

			switch (this.tableSchema.getKey().getCryptoType()) {
				case STD:
//						step 1 : get all stored values
					ResultScanner rs = super.getScanner(new Scan());
//						step 2 : check if specified row exists
					for(Result r = rs.next(); r != null; r = rs.next()) {
						byte[] resultRow = this.cryptoProperties.decodeRow(r.getRow());
						row = Utils.removePadding(row);
						if (Arrays.equals(row, resultRow)) {
//								Get the stored value for the specified family and qualifier and check if it's equal to a given value
							byte[] encryptedValue = r.getValue(family, qualifier);
							if(encryptedValue != null && encryptedValue.length > 0) {
								byte[] resultValue = this.cryptoProperties.decodeValue(family, qualifier, encryptedValue);
								if(Arrays.equals(value, resultValue)) {
//										If the values match, build and encrypted put
									Put encryptedPut;
									if(!put.isEmpty()) {
										encryptedPut = new Put(r.getRow());
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
				case PLT:
				case DET:
				case OPE:
				case FPE:
//						step 1 : encrypt row and value
					byte[] encryptedRow = this.cryptoProperties.encodeRow(row);
					byte[] encryptedValue;
					if(cryptoProperties.tableSchema.getCryptoTypeFromQualifier(new String(family), new String(qualifier)) == CryptoTechnique.CryptoType.STD) {
						Result encryptedResult = super.get(new Get(encryptedRow));
						Result temp_result = this.cryptoProperties.decodeResult(row, encryptedResult);
						byte[] temp_val = temp_result.getValue(family, qualifier);
						value = Utils.removePadding(value);
						if(Arrays.equals(temp_val, value)) {
							encryptedValue = encryptedResult.getValue(family, qualifier);
						}
						else {
							throw new NullPointerException("Specified value does not match with the stored version.");
						}
					}
					else {
						encryptedValue = this.cryptoProperties.encodeValue(family, qualifier, value);
					}

//						step 2 : encrypt put
					Put encryptedPut;
					if(!put.isEmpty()) {
						encryptedPut = new Put(this.cryptoProperties.encodeRow(put.getRow()));
						this.htableUtils.encryptCells(put.cellScanner(), this.tableSchema, encryptedPut, this.cryptoProperties);
					}
					else {
						throw new NullPointerException("Put object cannot be null");
					}
//						step 3 : call super
					operationPerformed = super.checkAndPut(encryptedRow, family, qualifier, encryptedValue, encryptedPut);
					break;
				default:
					break;
			}
		} catch (IOException e) {
			LOG.error("Exception in checkAndPut method. "+e.getMessage());
		}
		return operationPerformed;
	}

	@Override
	public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) {
		LOG.debug("IncrementColumnValue operation started.");
		long operationValue = 0;
		try {
			if(row.length == 0) {
				throw new NullPointerException("Row Key cannot be null.");
			}
			if(family == null) {
				throw new NullPointerException("Column family cannot be null.");
			}
			if(qualifier == null) {
				throw new NullPointerException("Column qualifier cannot be null.");
			}
			String temp_family = new String(family);
			String temp_qualifier = new String(qualifier);

////			In case of default schema, verify and/or create both family and qualifier instances in TableSchema
//			if(!SCHEMA_FILE) {
//				this.htableUtils.createDynamicColumnsForAtomicOperations(this.qEngine, this.tableSchema, temp_family, temp_qualifier);
//			}
//			else {
//				if(!this.tableSchema.containsQualifier(temp_family, temp_qualifier)) {
//					throw new NullPointerException("Column qualifier "+temp_family+":"+temp_qualifier+" not defined in schema file.");
//				}
//			}

			switch (this.tableSchema.getCryptoTypeFromQualifier(temp_family, temp_qualifier)) {
				case PLT:
					if(this.cryptoProperties.tableSchema.getKey().getCryptoType() != CryptoTechnique.CryptoType.STD) {
						operationValue = super.incrementColumnValue(this.cryptoProperties.encodeRow(row), family, qualifier, amount);
					}
					else {
						ResultScanner stdScanner = super.getScanner(new Scan());
						for(Result r = stdScanner.next(); r != null; r = stdScanner.next()) {
							byte[] aux = this.cryptoProperties.decodeRow(r.getRow());
							row = Utils.removePadding(row);

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
		} catch (IOException e) {
			LOG.error("Exception in incrementColumnValue method. "+e.getMessage());
		}
		return operationValue;
	}

	@Override
	public NavigableMap<HRegionInfo, ServerName> getRegionLocations() {
		try {
			return super.getRegionLocations();
		} catch (IOException e) {
			LOG.error("Exception in getRegionLocations method. "+e.getMessage());
			return null;
		}
	}

	@Override
	public HRegionLocation getRegionLocation(byte[] row) {
		LOG.debug("GetRegionLocation operation started.");
		try {
			if(row.length == 0) {
				throw new NullPointerException("Row Key cannot be null.");
			}

			switch(this.tableSchema.getKey().getCryptoType()) {
				case STD:
					HRegionLocation stdHRegionLocation = null;
					ResultScanner rs = super.getScanner(new Scan());
					for(Result r = rs.next(); r != null; r = rs.next()) {
						if(!r.isEmpty()) {
							byte[] temp_row = this.cryptoProperties.decodeRow(r.getRow());
							row = Utils.removePadding(row);
							if(Arrays.equals(temp_row, row)) {
								stdHRegionLocation = super.getRegionLocation(r.getRow());
								break;
							}
						}
					}
					return stdHRegionLocation;
				case PLT:
				case DET:
				case OPE:
				case FPE:
					return super.getRegionLocation(this.cryptoProperties.encodeRow(row));
				default:
					return null;
			}
		} catch (IOException e) {
			LOG.error("Exception in getRegionLocation method. "+e.getMessage());
			return null;
		}
	}

	@Override
	public Result getRowOrBefore(byte[] row, byte[] family) {
		LOG.debug("GetRowOrBefore operation started.");
		try {
			if (row.length == 0) {
				throw new NullPointerException("Row Key cannot be null.");
			}
			if (family == null) {
				throw new NullPointerException("Column family cannot be null.");
			}

////			In case of default schema, verify and/or create both family and qualifier instances in TableSchema
//			if(!SCHEMA_FILE) {
//				this.htableUtils.createDynamicColumnsForAtomicOperations(this.qEngine, this.tableSchema, new String(family), null);
//			}
//			else {
//				if(!this.tableSchema.containsFamily(new String(family))) {
//					throw new NullPointerException("Column family "+new String(family)+" not defined in schema file.");
//				}
//			}

			switch(this.tableSchema.getKey().getCryptoType()) {

				case STD:
				case DET:
				case FPE:
					ResultScanner rs = super.getScanner(new Scan());
					byte[] encryptedRowBefore = null;
					byte[] plaintextRowBefore = null;
					byte[] decodedRow;
					Result encResult = null;
					Bytes.ByteArrayComparator bcomp = new Bytes.ByteArrayComparator();
					for(Result r = rs.next(); r != null; r = rs.next()) {
						decodedRow = this.cryptoProperties.decodeRow(r.getRow());
						row = Utils.removePadding(row);
						if(Arrays.equals(decodedRow, row)) {
							encResult = super.getRowOrBefore(r.getRow(), family);
							break;
						} else {
							if(plaintextRowBefore == null) {
								encryptedRowBefore = r.getRow();
								plaintextRowBefore = decodedRow.clone();
							} else if(bcomp.compare(row, decodedRow) > 0 && bcomp.compare(plaintextRowBefore, decodedRow) < 0) {
								plaintextRowBefore = decodedRow.clone();
								encryptedRowBefore = r.getRow();
							}
						}
					}

					if(encResult == null) {
						encResult = super.getRowOrBefore(encryptedRowBefore, family);
					}

					return this.cryptoProperties.decodeResult(this.cryptoProperties.decodeRow(encResult.getRow()), encResult);
				case PLT:
				case OPE:
					Result encryptedResult = super.getRowOrBefore(this.cryptoProperties.encodeRow(row), family);
					return this.cryptoProperties.decodeResult(this.cryptoProperties.decodeRow(encryptedResult.getRow()), encryptedResult);
				default:
					return null;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}


}

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
import pt.uminho.haslab.safecloudclient.queryengine.QEngineIntegration;
import pt.uminho.haslab.safecloudclient.schema.Family;
import pt.uminho.haslab.safecloudclient.schema.SchemaParser;
import pt.uminho.haslab.safecloudclient.schema.TableSchema;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;

/**
 * CryptoTable class.
 * Provides a secure extended version of an HBase HTable
 */
public class CryptoTable extends HTable {
	static final Log LOG = LogFactory.getLog(CryptoTable.class.getName());
	static boolean SCHEMA_FILE = false;

	public CryptoProperties cryptoProperties;
	public ResultScannerFactory resultScannerFactory;
	public TableSchema tableSchema;
	public QEngineIntegration qEngine;
	public HTableFeaturesUtils htableUtils;
	public SecureFilterConverter secureFilterConverter;


	public CryptoTable(Configuration conf, String tableName, String schemaFilename) throws IOException {
		super(conf, TableName.valueOf(tableName));
		this.tableSchema = this.init(schemaFilename, tableName);
		this.cryptoProperties = new CryptoProperties(this.tableSchema);
		this.resultScannerFactory = new ResultScannerFactory();
		this.secureFilterConverter = new SecureFilterConverter(this.cryptoProperties);
		this.htableUtils = new HTableFeaturesUtils(this.cryptoProperties, this.secureFilterConverter);
	}

	public CryptoTable(Configuration conf, String tableName, TableSchema schema) throws IOException {
		super(conf, TableName.valueOf(tableName));
		this.tableSchema = schema;
		this.cryptoProperties = new CryptoProperties(this.tableSchema);
		this.resultScannerFactory = new ResultScannerFactory();
		this.secureFilterConverter = new SecureFilterConverter(this.cryptoProperties);
		this.htableUtils = new HTableFeaturesUtils(this.cryptoProperties, this.secureFilterConverter);
	}

	public CryptoTable(Configuration conf, String tableName) throws IOException {
		super(conf, TableName.valueOf(tableName));
//		Too much hardcoded
		File file = new File("src/main/resources/schema.xml");

//		Too much hardcoded
		if(file.isFile() && file.getName().equals("schema.xml")) {
			SCHEMA_FILE = true;
			this.tableSchema = this.init(file.getPath(), tableName);
		} else {
			SCHEMA_FILE = false;
			HBaseAdmin ha = new HBaseAdmin(conf);
			HTableDescriptor descriptor = ha.getTableDescriptor(TableName.valueOf(tableName));
			HColumnDescriptor[] columnDescriptors = descriptor.getColumnFamilies();

			this.qEngine = new QEngineIntegration();
			this.tableSchema = this.qEngine.buildQEngineTableSchema(tableName, columnDescriptors);
		}

		this.cryptoProperties = new CryptoProperties(this.tableSchema);

//		Too much hardcoded
//		While the cryptographic keys management is not defined, read keys from specific files
//		arrange cryptographic keys properties
		File cryptographicKey = new File("src/main/resources/key.txt");
		byte[] key;
		if(cryptographicKey.isFile() && cryptographicKey.getName().equals("key.txt")) {
			System.out.println("File key available.");
			key = Utils.readKeyFromFile(cryptographicKey.getPath());
		}
		else {
			System.out.println("No key available. Default key used.");
			key = new byte[]{(byte) 0x2B, (byte) 0x7E, (byte) 0x15, (byte) 0x16, (byte) 0x28, (byte) 0xAE, (byte) 0xD2,
					(byte) 0xA6, (byte) 0xAB, (byte) 0xF7, (byte) 0x15, (byte) 0x88, (byte) 0x09, (byte) 0xCF,
					(byte) 0x4F, (byte) 0x3C};
		}

//		Since the schema can be dynamically generated or build by a schema file, the Cryptographic Keys must be set
//		in different ways. In case of schema file, all cryptographic keys are set. In case of default usage, only the OPE
//		Key is set.
		if(SCHEMA_FILE) {
//			WARNING: missing FPE instantiation
			this.cryptoProperties.setKey(CryptoTechnique.CryptoType.STD, key);
			this.cryptoProperties.setKey(CryptoTechnique.CryptoType.DET, key);
			this.cryptoProperties.setKey(CryptoTechnique.CryptoType.OPE, key);
		}
		else {
			this.cryptoProperties.setKey(CryptoTechnique.CryptoType.OPE, key);
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
	public TableSchema init(String filename, String tablename) {
		if (filename == null) {
			throw new NullPointerException("Schema file name cannot be null.");
		}
		System.out.println("Fiz init table schema: ");
		SchemaParser schemaParser = new SchemaParser();
		schemaParser.parse(filename);
		System.out.println(schemaParser.getSchemas().get("usertable").toString());
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

//			Acknowledge the existing qualifiers
			if(!SCHEMA_FILE) {
				this.cryptoProperties.dynamicHColumnDescriptorsAddition(put.getFamilyCellMap(), this.qEngine);
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
//				Acknowledge the existing qualifiers
				if(!SCHEMA_FILE) {
					this.cryptoProperties.dynamicHColumnDescriptorsAddition(p.getFamilyCellMap(), this.qEngine);
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
		Result getResult = Result.EMPTY_RESULT;

		try {
			byte[] row = get.getRow();
			if(row.length == 0) {
				throw new NullPointerException("Row Key cannot be null.");
			}

//			Acknowledge the existing qualifiers
			if(!SCHEMA_FILE) {
				this.cryptoProperties.dynamicHColumnDescriptorsAddition(get.getFamilyMap(), this.qEngine);
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
					}

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

//			Acknowledge the existing qualifiers
			if(!SCHEMA_FILE) {
				this.cryptoProperties.dynamicHColumnDescriptorsAddition(g.getFamilyMap(), this.qEngine);
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
		try {
			byte[] row = delete.getRow();
			if(row.length == 0) {
				throw new NullPointerException("Row Key cannot be null.");
			}

//			Acknowledge the existing qualifiers
			if(!SCHEMA_FILE) {
				this.cryptoProperties.dynamicHColumnDescriptorsAddition(delete.getFamilyCellMap(), this.qEngine);
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

	@Override
	public void delete(List<Delete> deletes) {
		try {
			List<Delete> encryptedDeletes = new ArrayList<>(deletes.size());
			for (Delete del : deletes) {
				byte[] row = del.getRow();
				if (row.length == 0) {
					throw new NullPointerException("Row Key cannot be null.");
				}

//				Acknowledge the existing qualifiers
				if(!SCHEMA_FILE) {
					this.cryptoProperties.dynamicHColumnDescriptorsAddition(del.getFamilyCellMap(), this.qEngine);
				}

				List<String> cellsToDelete = this.htableUtils.deleteCells(del.cellScanner());

//			Verify the row key CryptoBox
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

//			Acknowledge the existing qualifiers
			if(!SCHEMA_FILE) {
				this.cryptoProperties.dynamicHColumnDescriptorsAddition(scan.getFamilyMap(), this.qEngine);
			}

//			Transform the original object in an encrypted scan.
			Scan encScan = this.htableUtils.buildEncryptedScan(scan);
			encScan.setCaching(scan.getCaching());
			encScan.setBatch(scan.getBatch());
			encScan.setCacheBlocks(scan.getCacheBlocks());

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

	@Override
	public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) {
		boolean operationPerformed = false;
		System.out.println("Secure Check and Put");
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

//			In case of default schema, verify and/or create both family and qualifier instances in TableSchema
			if(!SCHEMA_FILE) {
				this.htableUtils.createDynamicColumnsForAtomicOperations(this.qEngine, this.tableSchema, new String(family), new String(qualifier));
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
//					TODO testar isto a fundo
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
			System.out.println("Exception in checkAndPut method. "+e.getMessage());
			LOG.error("Exception in checkAndPut method. "+e.getMessage());
		}
		return operationPerformed;
	}

	@Override
	public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) {
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

//			In case of default schema, verify and/or create both family and qualifier instances in TableSchema
			if(!SCHEMA_FILE) {
				this.htableUtils.createDynamicColumnsForAtomicOperations(this.qEngine, this.tableSchema, temp_family, temp_qualifier);
			}
			else {
				if(!this.tableSchema.containsQualifier(temp_family, temp_qualifier)) {
					throw new NullPointerException("Column qualifier "+temp_family+":"+temp_qualifier+" not defined in schema file.");
				}
			}

			switch (this.tableSchema.getCryptoTypeFromQualifier(temp_family, temp_qualifier)) {
				case PLT:
//					TODO testar isto a fundo
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
			System.out.println("Exception in incrementColumnValue method. "+e.getMessage());
			LOG.error("Exception in incrementColumnValue method. "+e.getMessage());
		}
		return operationValue;
	}

	@Override
	public NavigableMap<HRegionInfo, ServerName> getRegionLocations() {
		try {
			return super.getRegionLocations();
		} catch (IOException e) {
			System.out.println("Exception in getRegionLocations method. "+e.getMessage());
			LOG.error("Exception in getRegionLocations method. "+e.getMessage());
			return null;
		}
	}

	@Override
	public HRegionLocation getRegionLocation(byte[] row) {
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
			System.out.println("Exception in getRegionLocation method. "+e.getMessage());
			LOG.error("Exception in getRegionLocation method. "+e.getMessage());
			return null;
		}
	}

//	TODO testar isto a fundo
	@Override
	public Result getRowOrBefore(byte[] row, byte[] family) {
		try {
			if (row.length == 0) {
				throw new NullPointerException("Row Key cannot be null.");
			}
			if (family == null) {
				throw new NullPointerException("Column family cannot be null.");
			}

//			In case of default schema, verify and/or create both family and qualifier instances in TableSchema
			if(!SCHEMA_FILE) {
				this.htableUtils.createDynamicColumnsForAtomicOperations(this.qEngine, this.tableSchema, new String(family), null);
			}
			else {
				if(!this.tableSchema.containsFamily(new String(family))) {
					throw new NullPointerException("Column family "+new String(family)+" not defined in schema file.");
				}
			}

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

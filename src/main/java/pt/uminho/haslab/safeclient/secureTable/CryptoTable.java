package pt.uminho.haslab.safeclient.secureTable;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.hbaseInterfaces.ExtendedHTable;
import pt.uminho.haslab.safeclient.ExtendedHTableImpl;
import pt.uminho.haslab.safeclient.secureTable.resultscanner.ResultScannerFactory;
import pt.uminho.haslab.safeclient.secureTable.securefilterfactory.SecureFilterConverter;
import pt.uminho.haslab.safeclient.shareclient.SharedTable;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.DatabaseSchema.CryptoType;
import pt.uminho.haslab.safemapper.Key;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smpc.exceptions.InvalidNumberOfBits;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * CryptoTable class.
 * Provides a secure extended version of an HBase HTable
 */
public class CryptoTable implements ExtendedHTable {

    static final Log LOG = LogFactory.getLog(CryptoTable.class.getName());
    private static final Lock lock = new ReentrantLock();
    private static Map<String, Object> databaseDefaultProperties;
    private static DatabaseSchema databaseSchema;
    private static boolean parsingComplete = false;
    private static boolean keyAcknowledgement = false;
    private static byte[] cryptographicKey;
    private CryptoProperties cryptoProperties;
    private ResultScannerFactory resultScannerFactory;
    private HTableFeaturesUtils htableUtils;
    private SecureFilterConverter secureFilterConverter;
    private TableSchema tableSchema;
    private ExtendedHTable htable;


    public CryptoTable(Configuration conf, String tableName) throws IOException, InvalidNumberOfBits {
        initializeResources(conf, tableName, null);
    }

    public CryptoTable(Configuration conf, String tableName, TableSchema schema) throws IOException, InvalidNumberOfBits {
        initializeResources(conf, tableName, schema);
    }

    public void initializeResources(Configuration conf, String tableName, TableSchema schema) throws IOException, InvalidNumberOfBits {
        String tableType = conf.get("baseTable");
        String schemaProperty = conf.get("schema");

        if (LOG.isDebugEnabled()) {
            LOG.debug("Schema is " + schemaProperty);
            LOG.debug("Base table is " + tableType);
        }

        if (schema != null) {
            this.tableSchema = schema;
            this.cryptoProperties = new CryptoProperties(this.tableSchema);

        } else {
            this.setSchema(conf, schemaProperty, tableName, tableType);
        }

        if (tableType.equals("HTable")) {
            LOG.debug("HTable create");
            htable = new ExtendedHTableImpl(conf, tableName);
        } else if (tableType.equals("SharedTable")) {
            LOG.debug("SharedTable create");
            htable = new SharedTable(conf, tableName, tableSchema);
        }

        /* While the cryptographic keys management is not defined, read keys from specific files
         * arrange cryptographic keys properties
         */
        this.setCryptographicKey(conf);

        for (CryptoType cryptoType : this.tableSchema.getEnabledCryptoTypes()) {
            this.cryptoProperties.setKey(cryptoType, cryptographicKey);
        }

        // this is common for both cases
        this.resultScannerFactory = new ResultScannerFactory();
        this.secureFilterConverter = new SecureFilterConverter(this.cryptoProperties);
        this.htableUtils = new HTableFeaturesUtils(this.cryptoProperties, this.secureFilterConverter);
    }

    private void setSchema(Configuration conf, String schemaProperty, String tableName, String tableType) throws FileNotFoundException, UnsupportedEncodingException {
        if (schemaProperty != null && !schemaProperty.isEmpty()) {
            while (!parsingComplete) {
                try {
                    lock.lock();
                    if (!parsingComplete) {
                        File file = new File(schemaProperty);
                        // TODO: jtpaulo - Qual era o problema c/ a leitura da chave e do configuration file.
                        databaseSchema = new DatabaseSchema(file.getPath());
                        databaseDefaultProperties = new HashMap<>();
                        databaseDefaultProperties = databaseSchema.getDatabaseDefaultProperties();
                        parsingComplete = true;
                        if (tableType.equals("SharedTable")) {
                            SharedTable.initializeThreadPool(conf.getInt("sharedClient.ThreadPool.size", 50));
                        }
                    }

                } finally {
                    lock.unlock();
                }
            }

            if (databaseSchema.containsKey(tableName)) {
                this.tableSchema = getTableSchema(tableName);
            } else {
                this.tableSchema = generateDefaultTableSchema(tableName);
                LOG.debug("Generate Default Table Schema for " + tableName + " table: " + tableSchema.toString());
            }

            this.cryptoProperties = new CryptoProperties(this.tableSchema);

        } else {
            LOG.error("Schema file not found");
            throw new FileNotFoundException("Schema file not found.");
        }
    }

    // TODO: esta parte depois tem de ser feita com um gestor de chaves
    // FIXME: The current approach instantiate the same cryptograhic key for all CryptoBox. Solution: Provide different cryptographic keys for each CryptoBox
    private void setCryptographicKey(Configuration conf) throws FileNotFoundException {
        String cryptographicKeyProperty = conf.get("cryptographickey");
        if (cryptographicKeyProperty != null && !cryptographicKeyProperty.isEmpty()) {
            while (!keyAcknowledgement) {
                try {
                    lock.lock();
                    if (!keyAcknowledgement) {
                        String keyPath = ClassLoader.getSystemResource(cryptographicKeyProperty).getPath();
                        LOG.debug("Load key in path " + keyPath);
                        File keyFile = new File(keyPath);

                        if (keyFile.isFile() && keyFile.getName().equals("key.txt")) {
                            LOG.debug("File key.");
                            cryptographicKey = Utils.readKeyFromFile(keyPath);
                        } else {
                            throw new FileNotFoundException("The file " + cryptographicKeyProperty + " does not match with the requirements.");
                        }
                        keyAcknowledgement = true;
                    }
                } catch (Exception e) {
                    LOG.error(e);
                    throw new IllegalStateException(e);
                } finally {
                    lock.unlock();
                }

            }
        } else {
            String error = "Cryptographic Key file not found.";
            LOG.error(error);
            throw new FileNotFoundException(error);
        }

    }


    private TableSchema getTableSchema(String tableName) {
        return databaseSchema.getTableSchema(tableName);
    }

    private TableSchema generateDefaultTableSchema(String tablename) {
        TableSchema temp = new TableSchema();
        temp.setTablename(tablename);
        temp.setDefaultKeyCryptoType((CryptoType) databaseDefaultProperties.get("defaultPropertiesKey"));
        temp.setDefaultColumnsCryptoType((CryptoType) databaseDefaultProperties.get("defaultPropertiesColumns"));
        temp.setDefaultKeyFormatSize((Integer) databaseDefaultProperties.get("defaultPropertiesKeyFormatSize"));
        temp.setDefaultColumnFormatSize((Integer) databaseDefaultProperties.get("defaultPropertiesColFormatSize"));
        temp.setDefaultKeyPadding((Boolean) databaseDefaultProperties.get("defaultPropertiesKeyPadding"));
        temp.setDefaultColumnPadding((Boolean) databaseDefaultProperties.get("defaultPropertiesColumnPadding"));
        temp.setEncryptionMode((Boolean) databaseDefaultProperties.get("defaultPropertiesEncryptionMode"));
        // KEY
        // FIXME: does not contemplate FPE
        temp.setKey(
                new Key(
                        (CryptoType) databaseDefaultProperties.get("defaultPropertiesKey"),
                        (Integer) databaseDefaultProperties.get("defaultPropertiesKeyFormatSize"),
                        (Boolean) databaseDefaultProperties.get("defaultPropertiesKeyPadding")));
        return temp;
    }


//----------------------------------------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------------------------------------

    /**
     * put(Put put) method : secure put/update method.
     * The original put object sends a set of qualifiers and values to insert in the database system.
     * Before the insertion both key and values are encrypted, following the database schema specifications.
     * In case of OPE CryptoBox, an additional qualifier is created and stores the respective value encrypted with the STD CryptoBox
     *
     * @param put original put object that contains the key, values, qualifiers, ...
     */
    @Override
    public void put(Put put) {
        LOG.debug("Put operation");
        Put finalPut;
        try {
            if (this.tableSchema.getEncryptionMode()) {
                finalPut = this.encodePutObject(put);
            } else {
                finalPut = put;
            }

            htable.put(finalPut);

        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void put(List<Put> puts) {
        LOG.debug("Batch put operation");
        List<Put> finalPutList;
        try {
            if (this.tableSchema.getEncryptionMode()) {
                finalPutList = new ArrayList<>(puts.size());
                for (Put p : puts) {
                    finalPutList.add(this.encodePutObject(p));
                }
            } else {
                finalPutList = puts;
            }

            htable.put(finalPutList);

        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new IllegalStateException(e);
        }
    }

    /**
     * get(Get get) method : secure get method.
     * The original get object sets the Row-Key to search in the database system. Before the get operation, the Row-Key
     * is encrypted accordingly the respective CryptoBox and its issued. After the server response, all the values must be
     * decoded with the respective CryptoBox, resulting in the original values stored by the user.
     *
     * @param get original get object that contains the key to perform the operation.
     * @return Result containing the plaintext value of the get result.
     */
    @Override
    public Result get(Get get) {
        LOG.debug("Get operation");
        Result getResult = Result.EMPTY_RESULT;
        try {
            if (this.tableSchema.getEncryptionMode()) {
                byte[] row = this.htableUtils.getObjectRow(get);
                Map<byte[], List<byte[]>> columns = this.cryptoProperties.getHColumnDescriptors(get.getFamilyMap());

                Object encodedGet = this.encodeGet(row, columns);

                switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
                    case STD:
                        getResult = this.decodeGetObject((ResultScanner) encodedGet, row);
                        break;
                    case SMPC:
                    case ISMPC:
                    case LSMPC:
                    case PLT:
                    case DET:
                    case OPE:
                    case FPE:
                        Result encryptedResult = htable.get((Get) encodedGet);
                        getResult = this.decodeGetObject(encryptedResult, row);
                        break;
                    default:
                        break;
                }

            } else {
                getResult = htable.get(get);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new IllegalStateException(e);
        }

        return getResult;
    }

    @Override
    public Result[] get(List<Get> gets) {
        LOG.debug("Batch get operation");
        Result[] results = new Result[gets.size()];
        try {
            if (this.tableSchema.getEncryptionMode()) {
                List<Get> encryptedGets = new ArrayList<>(gets.size());

                for (Get g : gets) {
                    byte[] row = this.htableUtils.getObjectRow(g);
                    Map<byte[], List<byte[]>> columns = this.cryptoProperties.getHColumnDescriptors(g.getFamilyMap());

                    // First phase: Encrypt all get objects
                    switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
                        case PLT:
                        case SMPC:
                        case ISMPC:
                        case LSMPC:
                        case DET:
                        case OPE:
                        case FPE:
                            Get encGet = new Get(this.cryptoProperties.encodeRow(row));
                            this.htableUtils.wrapHColumnDescriptors(encGet, columns);
                            encryptedGets.add(encGet);

                            break;
                        case STD:
                        default:
                            break;
                    }
                }

                // Second phase: call super() to batch the Get's List
                switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
                    case STD:
                        ResultScanner encryptedScan = htable.getScanner(new Scan());
                        int noMoreGetObjects = 0;

                        for (Result r = encryptedScan.next(); r != null; r = encryptedScan.next()) {
                            if (noMoreGetObjects != gets.size()) {
                                byte[] resultRow = this.cryptoProperties.decodeRow(r.getRow());
                                for (Get get : gets) {
                                    if (Arrays.equals(resultRow, this.htableUtils.getObjectRow(get))) {
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
                    case SMPC:
                    case ISMPC:
                    case LSMPC:
                    case DET:
                    case OPE:
                    case FPE:
                        Result[] encryptedResults = htable.get(encryptedGets);
                        for (int i = 0; i < encryptedResults.length; i++) {
                            // Third phase: decode result
                            results[i] = this.decodeGetObject(encryptedResults[i]);
                        }
                        break;
                    default:
                        break;
                }
            } else {
                results = htable.get(gets);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new IllegalStateException(e);
        }

        return results;
    }

    /**
     * delete(Delete delete) method : secure delete method.
     * The original delete object sets the Row-Key to search in the database system. Before the delete operation, the Row-Key
     * is encrypted accordingly the respective CryptoBox and its issued.
     *
     * @param delete original get object that contains the key to perform the operation.
     */
    @Override
    public void delete(Delete delete) {
        LOG.debug("Delete operation");
        try {
            if (this.tableSchema.getEncryptionMode()) {
                byte[] row = this.htableUtils.getObjectRow(delete);
                List<String> hcolumnsToDelete = this.htableUtils.deleteCells(delete.cellScanner());

                // Verify the Row-Key CryptoBox
                switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
                    case STD:
                        ResultScanner encScan = htable.getScanner(new Scan());
                        for (Result r = encScan.next(); r != null; r = encScan.next()) {
                            byte[] resultRowKey = this.cryptoProperties.decodeRow(r.getRow());
                            if (Arrays.equals(row, resultRowKey)) {
                                htable.delete(this.encodeDeleteObject(row, hcolumnsToDelete));
                                break;
                            }
                        }
                        break;
                    case SMPC:
                    case ISMPC:
                    case LSMPC:
                    case PLT:
                    case DET:
                    case OPE:
                    case FPE:
                        htable.delete(this.encodeDeleteObject(row, hcolumnsToDelete));
                        break;
                    default:
                        break;
                }
            } else {
                htable.delete(delete);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void delete(List<Delete> deletes) {
        LOG.debug("Batch delete operation");
        try {
            if (this.tableSchema.getEncryptionMode()) {
                List<Delete> encryptedDeletes = new ArrayList<>(deletes.size());
                CryptoType cType = this.cryptoProperties.tableSchema.getKey().getCryptoType();

                if (cType == CryptoType.STD) {
                    int noMoreDeleteObjects = 0;
                    ResultScanner encryptedScanner = htable.getScanner(new Scan());

                    for (Result r = encryptedScanner.next(); r != null; r = encryptedScanner.next()) {
                        if (noMoreDeleteObjects != deletes.size()) {
                            byte[] resultRow = this.cryptoProperties.decodeRow(r.getRow());
                            for (Delete delete : deletes) {
                                if (Arrays.equals(resultRow, this.htableUtils.getObjectRow(delete))) {
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
                        byte[] row = this.htableUtils.getObjectRow(del);
                        List<String> hcolumnsToDelete = this.htableUtils.deleteCells(del.cellScanner());
                        encryptedDeletes.add(this.encodeDeleteObject(row, hcolumnsToDelete));
                    }
                }

                htable.delete(encryptedDeletes);
            } else {
                htable.delete(deletes);
            }

        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new IllegalStateException(e);
        }
    }

    /**
     * getScanner(Scan scan) method : secure scan and filter operations
     * This operations provides the secure scan and filter operations over the database. Encrypting both start row, stop row
     * and compare value.
     *
     * @param scan scan object that provides the necessary filter and scan parameters.
     * @return resulting values that pass the filter parameters. The values are still encrypted.
     */
    @Override
    public ResultScanner getScanner(Scan scan) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getScanner operation");
        }
        ResultScanner result = null;
        try {
            if (this.tableSchema.getEncryptionMode()) {
                byte[] startRow = scan.getStartRow();
                byte[] endRow = scan.getStopRow();
                // Transform the original object in an encrypted scan.
                Scan encScan = this.htableUtils.buildEncryptedScan(scan);
                encScan.setCaching(scan.getCaching());

                ResultScanner encryptedResultScanner = htable.getScanner(encScan);
                // Return the corresponding result scanner to decrypt the resulting set of values
                result = this.resultScannerFactory.getResultScanner(
                        this.htableUtils.verifyFilterCryptoType(scan),
                        this.cryptoProperties,
                        startRow,
                        endRow,
                        encryptedResultScanner,
                        this.htableUtils.parseFilter(scan.getFilter()));

            } else {
                result = htable.getScanner(scan);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new IllegalStateException(e);
        }
        return result;
    }

    @Override
    public ResultScanner getScanner(byte[] bytes) throws IOException {
        throw new UnsupportedOperationException("Operation not supported.");
    }

    @Override
    public ResultScanner getScanner(byte[] bytes, byte[] bytes1) throws IOException {
        throw new UnsupportedOperationException("Operation not supported.");
    }

    @Override
    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier, byte[] value, Put put) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("CheckAndPut operation");
        }
        boolean operationPerformed = false;
        try {
            if (this.tableSchema.getEncryptionMode()) {
                this.htableUtils.verifyNullableByteArray(row);
                this.htableUtils.verifyNullableByteArray(family);
                this.htableUtils.verifyNullableByteArray(qualifier);
                this.htableUtils.verifyNullableByteArray(value);

                switch (this.tableSchema.getKey().getCryptoType()) {
                    case STD:
                        // step 1 : get all stored values
                        ResultScanner rs = htable.getScanner(new Scan());
                        // step 2 : check if specified row exists
                        for (Result r = rs.next(); r != null; r = rs.next()) {
                            byte[] resultRow = this.cryptoProperties.decodeRow(r.getRow());
                            if (Arrays.equals(row, resultRow)) {
                                // Get the stored value for the specified family and qualifier and check if it's equal to a given value
                                byte[] encryptedValue = r.getValue(family, qualifier);
                                if (encryptedValue != null && encryptedValue.length > 0) {
                                    byte[] resultValue = this.cryptoProperties.decodeValue(family, qualifier, encryptedValue);
                                    if (Arrays.equals(value, resultValue)) {
                                        // If the values match, build and encrypted put
                                        Put encryptedPut;
                                        if (!put.isEmpty()) {
                                            encryptedPut = new Put(r.getRow());
                                            this.htableUtils.encryptCells(put.cellScanner(), this.tableSchema, encryptedPut, this.cryptoProperties);
                                        } else {
                                            throw new NullPointerException("Put object cannot be null");
                                        }
                                        // Call super
                                        operationPerformed = htable.checkAndPut(r.getRow(), family, qualifier, encryptedValue, encryptedPut);
                                    }
                                } else {
                                    throw new NullPointerException("No matching Cell for the family and qualifier specified.");
                                }
                                break;
                            }
                        }
                        break;
                    case PLT:
                    case SMPC:
                    case ISMPC:
                    case LSMPC:
                    case DET:
                    case OPE:
                    case FPE:
                        // step 1 : encrypt row and value
                        byte[] encryptedRow = this.cryptoProperties.encodeRow(row);
                        byte[] encryptedValue;
                        if (cryptoProperties.tableSchema.getCryptoTypeFromQualifier(new String(family), new String(qualifier)) == CryptoType.STD) {
                            Result encryptedResult = htable.get(new Get(encryptedRow));
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

                        // step 2 : encrypt put
                        Put encryptedPut;
                        if (!put.isEmpty()) {
                            encryptedPut = new Put(this.cryptoProperties.encodeRow(put.getRow()));
                            this.htableUtils.encryptCells(put.cellScanner(), this.tableSchema, encryptedPut, this.cryptoProperties);
                        } else {
                            throw new NullPointerException("Put object cannot be null");
                        }
                        // step 3 : call super
                        operationPerformed = htable.checkAndPut(encryptedRow, family, qualifier, encryptedValue, encryptedPut);
                        break;
                    default:
                        break;
                }
            } else {
                operationPerformed = htable.checkAndPut(row, family, qualifier, value, put);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new IllegalStateException(e);
        }
        return operationPerformed;
    }

    @Override
    public long incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("incrementColumnValue operation");
        }
        long operationValue = 0;
        try {
            if (this.tableSchema.getEncryptionMode()) {
                this.htableUtils.verifyNullableByteArray(row);
                this.htableUtils.verifyNullableByteArray(family);
                this.htableUtils.verifyNullableByteArray(qualifier);

                String temp_family = new String(family);
                String temp_qualifier = new String(qualifier);

                switch (this.tableSchema.getCryptoTypeFromQualifier(temp_family, temp_qualifier)) {
                    case PLT:
                        if (this.cryptoProperties.tableSchema.getKey().getCryptoType() != CryptoType.STD) {
                            operationValue = htable.incrementColumnValue(this.cryptoProperties.encodeRow(row), family, qualifier, amount);
                        } else {
                            ResultScanner stdScanner = htable.getScanner(new Scan());
                            for (Result r = stdScanner.next(); r != null; r = stdScanner.next()) {
                                byte[] aux = this.cryptoProperties.decodeRow(r.getRow());
                                if (Arrays.equals(row, aux)) {
                                    operationValue = htable.incrementColumnValue(r.getRow(), family, qualifier, amount);
                                    break;
                                }
                            }
                        }
                        break;
                    case SMPC:
                    case ISMPC:
                    case LSMPC:
                    case STD:
                    case DET:
                    case OPE:
                    case FPE:
                        throw new UnsupportedOperationException("Secure operation not supported. Only for vanilla instance.");
                    default:
                        break;
                }
            } else {
                operationValue = htable.incrementColumnValue(row, family, qualifier, amount);
            }
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw new IllegalStateException(e);
        }
        return operationValue;
    }

    public HRegionLocation getRegionLocation(byte[] row) {
        LOG.debug("getRegionLocation operation");
        HRegionLocation hRegionLocation = null;
        try {
            if (this.tableSchema.getEncryptionMode()) {
                if (row.length == 0) {
                    throw new NullPointerException("Row-Key cannot be null.");
                }

                switch (this.tableSchema.getKey().getCryptoType()) {
                    case STD:
                        HRegionLocation stdHRegionLocation = null;
                        ResultScanner rs = htable.getScanner(new Scan());
                        for (Result r = rs.next(); r != null; r = rs.next()) {
                            if (!r.isEmpty()) {
                                byte[] temp_row = this.cryptoProperties.decodeRow(r.getRow());
                                if (Arrays.equals(temp_row, row)) {
                                    stdHRegionLocation = htable.getRegionLocation(r.getRow());
                                    break;
                                }
                            }
                        }
                        hRegionLocation = stdHRegionLocation;
                        break;
                    case SMPC:
                    case ISMPC:
                    case LSMPC:
                    case PLT:
                    case DET:
                    case OPE:
                    case FPE:
                        hRegionLocation = htable.getRegionLocation(this.cryptoProperties.encodeRow(row));
                        break;
                    default:
                        break;
                }
            } else {
                hRegionLocation = htable.getRegionLocation(row);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new IllegalStateException(e);
        }
        return hRegionLocation;
    }

    @Override
    public byte[][] getStartKeys() throws IOException {
        LOG.debug("getStartKeys operation");
        return this.htable.getStartKeys();
    }

    @Override
    public Pair getStartEndKeys() throws IOException {
        LOG.debug("getStartEndKeys operation");
        return this.htable.getStartEndKeys();
    }

    @Override
    public List getRegionsInRange(byte[] bytes, byte[] bytes1) throws IOException {
        LOG.debug("getRegionsInRange operation");
        return this.htable.getRegionsInRange(bytes, bytes1);
    }

    @Override
    public NavigableMap getRegionLocations() throws IOException {
        LOG.debug("getRegionLocations operation");
        return this.htable.getRegionLocations();
    }

    @Override
    public Result getRowOrBefore(byte[] row, byte[] family) {
        LOG.debug("GetRowOrBefore operation");
        Result result = Result.EMPTY_RESULT;
        try {
            if (this.tableSchema.getEncryptionMode()) {
                this.htableUtils.verifyNullableByteArray(row);
                this.htableUtils.verifyNullableByteArray(family);

                switch (this.tableSchema.getKey().getCryptoType()) {
                    case STD:
                    case DET:
                    case FPE:
                        ResultScanner rs = htable.getScanner(new Scan());
                        byte[] encryptedRowBefore = null;
                        byte[] plaintextRowBefore = null;
                        byte[] decodedRow;
                        Result encResult = null;
                        Bytes.ByteArrayComparator bcomp = new Bytes.ByteArrayComparator();
                        for (Result r = rs.next(); r != null; r = rs.next()) {
                            decodedRow = this.cryptoProperties.decodeRow(r.getRow());
                            if (Arrays.equals(decodedRow, row)) {
                                encResult = htable.getRowOrBefore(r.getRow(), family);
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
                            encResult = htable.getRowOrBefore(encryptedRowBefore, family);
                        }

                        result = this.cryptoProperties.decodeResult(this.cryptoProperties.decodeRow(encResult.getRow()), encResult);
                        break;
                    case SMPC:
                    case ISMPC:
                    case LSMPC:
                    case PLT:
                    case OPE:
                        Result encryptedResult = htable.getRowOrBefore(this.cryptoProperties.encodeRow(row), family);
                        result = this.cryptoProperties.decodeResult(this.cryptoProperties.decodeRow(encryptedResult.getRow()), encryptedResult);
                        break;
                    default:
                        break;
                }
            } else {
                result = htable.getRowOrBefore(row, family);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new IllegalStateException(e);
        }
        return result;
    }

//----------------------------------------------------------------------------------------------------------------------
//----------------------------------------------------------------------------------------------------------------------

    private Put encodePutObject(Put p) {
        byte[] row = this.htableUtils.getObjectRow(p);

//		Encode the Row-Key
        Put encryptedObject = new Put(this.cryptoProperties.encodeRow(row));
        this.htableUtils.encryptCells(p.cellScanner(), this.tableSchema, encryptedObject, this.cryptoProperties);
        return encryptedObject;
    }

    private Object encodeGet(byte[] row, Map<byte[], List<byte[]>> columns) {
        Object result = null;
        try {

            switch (this.cryptoProperties.tableSchema.getKey().getCryptoType()) {
                case STD:
                    Scan stdGetScan = new Scan();
                    this.htableUtils.wrapHColumnDescriptors(stdGetScan, columns);

                    result = htable.getScanner(stdGetScan);

                    break;
                case PLT:
                case SMPC:
                case ISMPC:
                case LSMPC:
                case DET:
                case OPE:
                case FPE:
                    Get encGet = new Get(this.cryptoProperties.encodeRow(row));
                    this.htableUtils.wrapHColumnDescriptors(encGet, columns);

                    result = encGet;

                    break;
                default:
                    break;
            }

        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new IllegalStateException(e);
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
                String errorMsg = "object to decode cannot be null";
                LOG.error(errorMsg);
                throw new NullPointerException(errorMsg);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new IllegalStateException(e);
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
                String errorMsg = "Unrecognized object type.";
                throw new NullPointerException(errorMsg);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new IllegalStateException(e);
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
                String errorMsg = "Unrecognized object type.";
                LOG.debug(errorMsg);
                throw new NullPointerException(errorMsg);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new IllegalStateException(e);
        }

        return getResult;
    }

    private Delete encodeDeleteObject(byte[] row, List<String> hcolumnsToDelete) {
        Delete encryptedObject = new Delete(this.cryptoProperties.encodeRow(row));
        this.htableUtils.wrapDeletedCells(hcolumnsToDelete, encryptedObject);
        return encryptedObject;
    }


    @Override
    public byte[] getTableName() {
        return htable.getTableName();
    }

    @Override
    public TableName getName() {
        return htable.getName();
    }

    @Override
    public Configuration getConfiguration() {
        return htable.getConfiguration();
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        LOG.debug("Get table Descriptor operator");
        return htable.getTableDescriptor();
    }

    @Override
    public boolean exists(Get get) throws IOException {
        LOG.debug("Exists operator");
        return htable.exists(get);
    }

    @Override
    public Boolean[] exists(List<Get> list) throws IOException {
        LOG.debug("Batch Exists operator");
        return htable.exists(list);
    }

    @Override
    public void batch(List<? extends Row> list, Object[] objects) throws IOException, InterruptedException {
        LOG.debug("Batch operator");
        htable.batch(list, objects);
    }

    @Override
    public Object[] batch(List<? extends Row> list) throws IOException, InterruptedException {
        LOG.debug("BatchOperator");
        return htable.batch(list);
    }

    @Override
    public <R> void batchCallback(List<? extends Row> list, Object[] objects, Batch.Callback<R> callback) throws IOException, InterruptedException {
        LOG.debug("BatchCallback operator");
        htable.batchCallback(list, objects, callback);
    }

    @Override
    public <R> Object[] batchCallback(List<? extends Row> list, Batch.Callback<R> callback) throws IOException, InterruptedException {
        LOG.debug("BatchCallBack operator");
        return htable.batchCallback(list, callback);
    }

    @Override
    public boolean checkAndDelete(byte[] bytes, byte[] bytes1, byte[] bytes2, byte[] bytes3, Delete delete) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    @Override
    public void mutateRow(RowMutations rowMutations) throws IOException {
        LOG.debug("MutateRow operator");
        htable.mutateRow(rowMutations);
    }

    @Override
    public Result append(Append append) throws IOException {
        LOG.debug("Append operator");
        return htable.append(append);
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        LOG.debug("increment operator");
        return htable.increment(increment);
    }

    @Override
    public long incrementColumnValue(byte[] bytes, byte[] bytes1, byte[] bytes2, long l, Durability durability) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public long incrementColumnValue(byte[] bytes, byte[] bytes1, byte[] bytes2, long l, boolean b) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isAutoFlush() {
        return htable.isAutoFlush();
    }

    @Override
    public void setAutoFlush(boolean b) {
        htable.setAutoFlush(b);
    }

    @Override
    public void flushCommits() throws IOException {
        LOG.debug("FlushCommits operator");
        htable.flushCommits();
    }

    @Override
    public void close() throws IOException {
        LOG.debug("Close operator");
        htable.close();
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] bytes) {
        return htable.coprocessorService(bytes);
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> aClass, byte[] bytes, byte[] bytes1, Batch.Call<T, R> call) throws ServiceException, Throwable {
        LOG.debug("coprocessorServerice operator");
        return htable.coprocessorService(aClass, bytes, bytes1, call);
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> aClass, byte[] bytes, byte[] bytes1, Batch.Call<T, R> call, Batch.Callback<R> callback) throws ServiceException, Throwable {
        LOG.debug("Coprocessor sErvice");
        htable.coprocessorService(aClass, bytes, bytes1, call, callback);
    }

    @Override
    public void setAutoFlush(boolean b, boolean b1) {
        htable.setAutoFlush(b, b1);
    }

    @Override
    public void setAutoFlushTo(boolean b) {
        htable.setAutoFlush(b);
    }

    @Override
    public long getWriteBufferSize() {
        return htable.getWriteBufferSize();
    }

    @Override
    public void setWriteBufferSize(long l) throws IOException {
        htable.setWriteBufferSize(l);
    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes1, R r) throws ServiceException, Throwable {
        LOG.debug("batchCoprocessorService");
        return htable.batchCoprocessorService(methodDescriptor, message, bytes, bytes1, r);
    }

    @Override
    public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes1, R r, Batch.Callback<R> callback) throws ServiceException, Throwable {
        LOG.debug("batchCoprocessorService");
        htable.batchCoprocessorService(methodDescriptor, message, bytes, bytes1, r, callback);
    }

    @Override
    public boolean checkAndMutate(byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp, byte[] bytes3, RowMutations rowMutations) throws IOException {
        LOG.debug("CheckAndMutate");
        return htable.checkAndMutate(bytes, bytes1, bytes2, compareOp, bytes3, rowMutations);
    }

}

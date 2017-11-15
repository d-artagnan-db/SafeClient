package pt.uminho.haslab.safeclient.helpers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;
import pt.uminho.haslab.safeclient.ExtendedHTable;
import pt.uminho.haslab.safemapper.Family;
import pt.uminho.haslab.safemapper.Qualifier;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.testingutils.ValuesGenerator;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public abstract class AbstractTableGenerator {


    protected static final Log LOG = LogFactory
            .getLog(AbstractTableGenerator.class.getName());

    protected static String VANILLA = "_VANILLA";

    protected TableSchema schema;

    protected Map<String, Map<String, ColType>> qualifierColTypes;

    protected Map<String, Map<String, List<byte[]>>> generatedValues;

    protected final List<byte[]> rowIdentifiers;

    public AbstractTableGenerator() {
        this.qualifierColTypes = new HashMap<>();
        this.generatedValues = new HashMap<>();
        this.rowIdentifiers = new ArrayList<byte[]>();
        schema = getTableSchema();
        defineColTypes();
    }


    private List<Put> generatePuts() {

        List<Put> puts = new ArrayList<Put>();

        for (int i = 0; i < getNumberOfRecords(); i++) {
            byte[] id = ("" + i).getBytes();
            Put put = new Put(id);

            List<Family> fams = this.schema.getColumnFamilies();

            for (Family fam : fams) {
                for (Qualifier qual : fam.getQualifiers()) {
                    byte[] val = null;

                    switch (getColType(fam.getFamilyName(), qual.getName())) {
                        case STRING:
                            val = ValuesGenerator.randomString(10).getBytes();
                            break;
                        case INT:
                            //val = ValuesGenerator.randomBigInteger(10).toByteArray();

                            boolean invalid = true;
                            do {
                                int intVal = ValuesGenerator.randomInt();
                                val = BigInteger.valueOf(intVal).toByteArray();
                                if (val.length == 4) {
                                    /** Some integer values are small and have a byte array of length 3. Since HBase
                                     * does a lexicographic comparision based on the byte array, if the size of the byte
                                     * array of every value are not the same, the comparision is not fair. HBase will
                                     * not return a correct result because the comparision is based on the bytes and not
                                     * on the numerical value. If the byte array of the number is smaller than three,
                                     * then generate a new one until it is valid.
                                     */
                                    invalid = false;
                                }
                            } while (invalid);
                            break;

                    }
                    put.add(fam.getFamilyName().getBytes(), qual.getName()
                            .getBytes(), val);

                    storeValueToPut(fam.getFamilyName(), qual.getName(), val);
                }
            }
            rowIdentifiers.add(id);
            puts.add(put);
        }
        return puts;
    }

    private ColType getColType(String fam, String qual) {
        return this.qualifierColTypes.get(fam).get(qual);
    }

    private void storeValueToPut(String cf, String cq, byte[] val) {

        if (!generatedValues.containsKey(cf)) {
            generatedValues.put(cf, new HashMap<String, List<byte[]>>());
        }

        if (!generatedValues.get(cf).containsKey(cq)) {
            generatedValues.get(cf).put(cq, new ArrayList<byte[]>());
        }
        generatedValues.get(cf).get(cq).add(val);
    }

    private HTableDescriptor generateTableDescriptor(String tableName) {
        TableName tbName = TableName.valueOf(tableName);
        HTableDescriptor table = new HTableDescriptor(tbName);
        for (Family fam : schema.getColumnFamilies()) {
            HColumnDescriptor family = new HColumnDescriptor(fam.getFamilyName());
            table.addFamily(family);
        }
        return table;
    }

    protected void compareResult(Result vanillaResult, Result protectedResult){
        byte[] normalRow = vanillaResult.getRow();
        byte[] protectedRow = protectedResult.getRow();
        assertArrayEquals(normalRow, protectedRow);

        for (Family fam : schema.getColumnFamilies()) {
            for (Qualifier qual : fam.getQualifiers()) {
                byte[] normalVal = vanillaResult.getValue(fam.getFamilyName().getBytes(), qual.getName().getBytes());
                byte[] safeVal = protectedResult.getValue(fam.getFamilyName().getBytes(), qual.getName().getBytes());
                LOG.debug("Comparing value of column " + fam.getFamilyName() + ":" + qual.getName());
                LOG.debug("Comparing value " + new BigInteger(normalVal) + " : " + new BigInteger(safeVal));
                assertArrayEquals(normalVal, safeVal);
            }
        }

    }
    private void validateResults(Scan scan, ExtendedHTable vanillaTable, ExtendedHTable protectedTable) {

        try{
            ResultScanner vanillaResultScanner = vanillaTable.getScanner(scan);
            ResultScanner protectedResultScanner = protectedTable.getScanner(scan);
            Result vanillaResult = vanillaResultScanner.next();
            Result protectedResult = protectedResultScanner.next();
            while(vanillaResult != null  && protectedResult != null ){
                compareResult(vanillaResult, protectedResult);
                vanillaResult = vanillaResultScanner.next();
                protectedResult = protectedResultScanner.next();

            }
        }catch(Exception ex){
            if(expectException()){
                LOG.debug("Expected exception");
                assertEquals(true, getExpectedExceptionNames().contains(ex.getClass().getName()));
            }else{
                LOG.debug(ex);
                throw new IllegalStateException(ex);
            }
        }
    }

    @Test
    public void test() throws Exception {

        AdminProxy protectedAdmin = getProtectedHBaseAdmin();
        AdminProxy vanillaAdmin = getVanillaAdmin();

        LOG.info("Create cluster");

        //The protected admin is the one always used to  start a cluster in unit tests
        protectedAdmin.startCluster(getProtectedSchemaPath());

        LOG.info("Initiate admin connections");

        //Initialize admin connections to cluster
        protectedAdmin.initalizeAdminConnection();
        vanillaAdmin.initalizeAdminConnection();

        //Create Tables for testing
        HTableDescriptor tableDesc = generateTableDescriptor(schema.getTablename());
        HTableDescriptor vanillaTableDesc = generateTableDescriptor(schema.getTablename() + VANILLA);

        LOG.info("Create tables");

        protectedAdmin.createTable(tableDesc);
        vanillaAdmin.createTable(vanillaTableDesc);

        LOG.info("Get HTable interface to tables");

        // Get HTable client interface for requests
        ExtendedHTable protectedTable = protectedAdmin.createTableInterface(tableDesc.getNameAsString(),  schema);
        ExtendedHTable vanillaTable = vanillaAdmin.createTableInterface(vanillaTableDesc.getNameAsString(), schema);

        // Insert records on tables
        List<Put> puts = generatePuts();
        LOG.info("Insert records");
        put(protectedTable, puts);
        put(vanillaTable, puts);

        // Custom test execution

        LOG.info("Additional test execution");
        additionalTestExecution(vanillaTableDesc, tableDesc);

        if (validateResults()) {
            LOG.info("ValidateResults");

            //Validate results with test defined scan
            Scan testScan = getTestScan();
            LOG.info(testScan);
            validateResults(testScan, vanillaTable, protectedTable);
        }

        LOG.info("Stop Cluster Execution");
        protectedAdmin.stopCluster();

    }

    protected abstract AdminProxy getProtectedHBaseAdmin() throws Exception;

    protected abstract AdminProxy getVanillaAdmin() throws IOException;

    protected abstract void testExecution(List<Put> puts) throws IOException;

    protected abstract void put(ExtendedHTable table, List<Put> puts) throws IOException;

    protected abstract void additionalTestExecution(HTableDescriptor vanillaTableDescriptor, HTableDescriptor protectedTableDescriptor) throws Exception;

    protected abstract Scan getTestScan();

    protected abstract boolean validateResults();

    protected abstract TableSchema getTableSchema();

    protected abstract int getNumberOfRecords();

    protected abstract String getProtectedSchemaPath();

    protected abstract void defineColTypes();

    protected abstract boolean expectException();

    protected abstract Set<String> getExpectedExceptionNames();
    public enum ColType {
        STRING, INT
    }

}

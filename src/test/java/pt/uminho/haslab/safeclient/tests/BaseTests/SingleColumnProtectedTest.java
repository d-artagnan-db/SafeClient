package pt.uminho.haslab.safeclient.tests.BaseTests;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import pt.uminho.haslab.hbaseInterfaces.ExtendedHTable;
import pt.uminho.haslab.safeclient.helpers.AbstractTableGenerator;
import pt.uminho.haslab.safeclient.helpers.AdminProxy;
import pt.uminho.haslab.safeclient.helpers.DefaultHBaseClient;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.Family;
import pt.uminho.haslab.safemapper.Qualifier;
import pt.uminho.haslab.safemapper.TableSchema;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public abstract class SingleColumnProtectedTest extends AbstractTableGenerator {


    protected Qualifier generateQualifier(DatabaseSchema.CryptoType type, String name, int formatSize, boolean padding) {
        Qualifier qual = new Qualifier();

        qual.setQualifierName(name);
        qual.setCryptoType(type);
        qual.setFormatSize(formatSize);
        qual.setPadding(padding);

        return qual;
    }

    protected abstract DatabaseSchema.CryptoType getProtectedColumnType();

    protected abstract int getProtectedColumnFormatSize();


    @Override
    protected TableSchema getTableSchema() {
        TableSchema schema = new TableSchema();

        Family fam = new Family();
        fam.setFamilyName("Teste");
        fam.setCryptoType(DatabaseSchema.CryptoType.PLT);

        Qualifier one = generateQualifier(DatabaseSchema.CryptoType.PLT, "Name", 100, false);
        Qualifier two = generateQualifier(getProtectedColumnType(), "Age", getProtectedColumnFormatSize(), false);

        fam.addQualifier(one);
        fam.addQualifier(two);

        schema.setTablename("Teste");
        schema.setDefaultKeyCryptoType(DatabaseSchema.CryptoType.PLT);
        schema.setDefaultColumnsCryptoType(DatabaseSchema.CryptoType.PLT);
        schema.setDefaultKeyPadding(false);
        schema.setDefaultColumnPadding(false);
        schema.setDefaultColumnFormatSize(10);
        schema.setDefaultKeyFormatSize(10);
        schema.setEncryptionMode(true);
        schema.addFamily(fam);
        return schema;

    }

    protected void defineColTypes() {
        this.qualifierColTypes.put("Teste", new HashMap<String, ColType>());
        this.qualifierColTypes.get("Teste").put("Name", ColType.STRING);
        this.qualifierColTypes.get("Teste").put("Age", ColType.INT);
    }


    @Override
    protected int getNumberOfRecords() {
        return 100;
    }

    @Override
    protected AdminProxy getVanillaAdmin() throws IOException {
        return new DefaultHBaseClient("def-hbase-client.xml");
    }

    @Override
    protected void testExecution(List<Put> puts) throws IOException {

    }

    @Override
    protected void put(ExtendedHTable table, List<Put> puts) throws Exception {

        for (Put p : puts) {
            table.put(p);
        }
    }

    @Override
    protected void additionalTestExecution(HTableDescriptor vanillaTableDescriptor, HTableDescriptor protectedTableDescriptor) throws Exception {

        ExtendedHTable vanillaTable = getVanillaAdmin().createTableInterface(vanillaTableDescriptor.getNameAsString(), schema);
        ExtendedHTable protectedTable = getProtectedHBaseAdmin().createTableInterface(protectedTableDescriptor.getNameAsString(), schema);

        for (byte[] rowID : rowIdentifiers) {
            Get get = new Get(rowID);
            Result vanillaRes = vanillaTable.get(get);
            Result protectedRes = protectedTable.get(get);
            compareResult(vanillaRes, protectedRes);
        }

    }

    @Override
    protected Scan getTestScan() {
        return new Scan();
    }

    @Override
    protected boolean validateResults() {
        return true;
    }

    protected boolean expectException() {
        return false;
    }

    protected Set<String> getExpectedExceptionNames() {
        return null;
    }

}

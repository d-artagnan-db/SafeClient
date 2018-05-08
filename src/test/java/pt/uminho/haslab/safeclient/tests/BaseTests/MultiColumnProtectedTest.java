package pt.uminho.haslab.safeclient.tests.BaseTests;

import org.junit.Before;
import pt.uminho.haslab.safeclient.helpers.AdminProxy;
import pt.uminho.haslab.safeclient.helpers.MultiCryptoClient;
import pt.uminho.haslab.safeclient.helpers.RedisUtils;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.Family;
import pt.uminho.haslab.safemapper.Qualifier;
import pt.uminho.haslab.safemapper.TableSchema;

import java.io.IOException;
import java.util.HashMap;

public class MultiColumnProtectedTest extends SingleColumnProtectedTest {

    @Before
    public void initializeRedisContainer() throws IOException {
        RedisUtils.flushAll("redis");
    }

    @Override
    protected DatabaseSchema.CryptoType getProtectedColumnType() {
        return null;
    }

    @Override
    protected int getProtectedColumnFormatSize() {
        return 0;
    }

    @Override
    protected AdminProxy getProtectedHBaseAdmin() throws Exception {
        return new MultiCryptoClient("smpc-protected-hbase-client.xml");
    }

    @Override
    protected TableSchema getTableSchema() {
        TableSchema schema = new TableSchema();

        Family fam = new Family();
        fam.setFamilyName("Teste");
        fam.setCryptoType(DatabaseSchema.CryptoType.PLT);

        Qualifier one = generateQualifier(DatabaseSchema.CryptoType.PLT, "Name", 100, false);
        Qualifier two = generateQualifier(DatabaseSchema.CryptoType.STD, "Age1", 4, false);
        Qualifier three = generateQualifier(DatabaseSchema.CryptoType.DET, "Age2", 4, false);
        Qualifier four = generateQualifier(DatabaseSchema.CryptoType.OPE, "Age3", 4, false);
        Qualifier five = generateQualifier(DatabaseSchema.CryptoType.SMPC, "Age4", 64, false);

        fam.addQualifier(one);
        fam.addQualifier(two);
        fam.addQualifier(three);
        fam.addQualifier(four);
        fam.addQualifier(five);

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

    @Override
    protected String getProtectedSchemaPath() {
        return "multi-put-get-schema.xml";
    }

    protected void defineColTypes() {
        this.qualifierColTypes.put("Teste", new HashMap<String, ColType>());
        this.qualifierColTypes.get("Teste").put("Name", ColType.STRING);
        this.qualifierColTypes.get("Teste").put("Age1", ColType.INT);
        this.qualifierColTypes.get("Teste").put("Age2", ColType.INT);
        this.qualifierColTypes.get("Teste").put("Age3", ColType.INT);
        this.qualifierColTypes.get("Teste").put("Age4", ColType.INT);
    }

}

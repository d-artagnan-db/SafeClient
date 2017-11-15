package pt.uminho.haslab.safeclient.tests.FilterList;

import org.junit.Before;
import pt.uminho.haslab.safeclient.helpers.AdminProxy;
import pt.uminho.haslab.safeclient.helpers.RedisUtils;
import pt.uminho.haslab.safeclient.tests.BaseTests.SingleColumnProtectedTest;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.Family;
import pt.uminho.haslab.safemapper.Qualifier;
import pt.uminho.haslab.safemapper.TableSchema;

import java.io.IOException;
import java.util.HashMap;

public abstract class MultiColumnCTypeTest extends SingleColumnProtectedTest {

    @Before
    public void initializeRedisContainer() throws IOException {
        RedisUtils.flushAll("redis");
    }

    @Override
    protected TableSchema getTableSchema() {
        TableSchema schema = new TableSchema();

        Family fam = new Family();
        fam.setFamilyName("Teste");
        fam.setCryptoType(DatabaseSchema.CryptoType.PLT);

        Qualifier one = generateQualifier(DatabaseSchema.CryptoType.PLT, "Name", 100, false);
        Qualifier two = generateQualifier(getProtectedColumnType(), "Age1", getProtectedColumnFormatSize(), false);
        Qualifier three = generateQualifier(getProtectedColumnType(), "Age2", getProtectedColumnFormatSize(), false);
        Qualifier four  = generateQualifier(getProtectedColumnType(), "Age3", getProtectedColumnFormatSize(), false);

        fam.addQualifier(one);
        fam.addQualifier(two);
        fam.addQualifier(three);
        fam.addQualifier(four);

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

    protected void defineColTypes(){
        this.qualifierColTypes.put("Teste", new HashMap<String, ColType>());
        this.qualifierColTypes.get("Teste").put("Name", ColType.STRING);
        this.qualifierColTypes.get("Teste").put("Age1", ColType.INT);
        this.qualifierColTypes.get("Teste").put("Age2", ColType.INT);
        this.qualifierColTypes.get("Teste").put("Age3", ColType.INT);
    }

}

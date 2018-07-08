package pt.uminho.haslab.safeclient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.Key;
import pt.uminho.haslab.safemapper.TableSchema;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class Database {

    static final Log LOG = LogFactory.getLog(Database.class.getName());

    private static DatabaseSchema databaseSchema;
    private static Map<String, Object> databaseDefaultProperties;


    public static synchronized TableSchema getTableSchema(Configuration conf, String tableName) {

        if (databaseSchema == null) {
            String schemaProperty = conf.get("schema");
            File file = new File(schemaProperty);
            databaseSchema = new DatabaseSchema(file.getPath());
            databaseDefaultProperties = new HashMap<>();
            databaseDefaultProperties = databaseSchema.getDatabaseDefaultProperties();
        }

        if (databaseSchema.containsKey(tableName)) {
            return databaseSchema.getTableSchema(tableName);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Generate Default Table Schema for " + tableName);
            }
            return generateDefaultTableSchema(tableName);
        }
    }

    private static TableSchema generateDefaultTableSchema(String tablename) {
        TableSchema temp = new TableSchema();
        temp.setTablename(tablename);
        temp.setDefaultKeyCryptoType((DatabaseSchema.CryptoType) databaseDefaultProperties.get("defaultPropertiesKey"));
        temp.setDefaultColumnsCryptoType((DatabaseSchema.CryptoType) databaseDefaultProperties.get("defaultPropertiesColumns"));
        temp.setDefaultKeyFormatSize((Integer) databaseDefaultProperties.get("defaultPropertiesKeyFormatSize"));
        temp.setDefaultColumnFormatSize((Integer) databaseDefaultProperties.get("defaultPropertiesColFormatSize"));
        temp.setDefaultKeyPadding((Boolean) databaseDefaultProperties.get("defaultPropertiesKeyPadding"));
        temp.setDefaultColumnPadding((Boolean) databaseDefaultProperties.get("defaultPropertiesColumnPadding"));
        temp.setEncryptionMode((Boolean) databaseDefaultProperties.get("defaultPropertiesEncryptionMode"));
        // KEY
        // FIXME: does not contemplate FPE
        temp.setKey(
                new Key(
                        (DatabaseSchema.CryptoType) databaseDefaultProperties.get("defaultPropertiesKey"),
                        (Integer) databaseDefaultProperties.get("defaultPropertiesKeyFormatSize"),
                        (Boolean) databaseDefaultProperties.get("defaultPropertiesKeyPadding")));
        return temp;
    }
}

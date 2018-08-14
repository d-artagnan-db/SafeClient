package pt.uminho.haslab.safeclient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import pt.uminho.haslab.safemapper.*;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static pt.uminho.haslab.safemapper.DatabaseSchema.CryptoType.LSMPC;
import static pt.uminho.haslab.safemapper.DatabaseSchema.CryptoType.SMPC;
import static pt.uminho.haslab.safemapper.DatabaseSchema.CryptoType.XOR;

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

    public static boolean requiresSharedTable(TableSchema schema){
        boolean requires = false;
        for(Family fam: schema.getColumnFamilies()){
            for(Qualifier qual: fam.getQualifiers()){
                DatabaseSchema.CryptoType ctype = qual.getCryptoType();
                if( ctype == SMPC || ctype  == LSMPC || ctype == XOR ){
                    requires = true;
                }
            }
        }
        return requires;

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

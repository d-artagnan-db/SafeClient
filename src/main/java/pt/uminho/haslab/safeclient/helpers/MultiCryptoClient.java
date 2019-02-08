package pt.uminho.haslab.safeclient.helpers;

import org.apache.hadoop.conf.Configuration;
import pt.uminho.haslab.hbaseInterfaces.ExtendedHTable;
import pt.uminho.haslab.safeclient.shareclient.SharedTable;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smpc.exceptions.InvalidNumberOfBits;

import java.io.IOException;

public class MultiCryptoClient extends ShareClient {

    public MultiCryptoClient(String configuration) throws IOException {
        super(configuration);
    }

    @Override
    public ExtendedHTable createTableInterface(String tableName, TableSchema schema)
            throws IOException, InvalidNumberOfBits {
        Configuration conf = new Configuration();
        conf.addResource(configuration);
        return new SharedTable(conf, tableName, schema);
    }
}

package pt.uminho.haslab.safeclient.helpers;

import org.apache.hadoop.conf.Configuration;
import pt.uminho.haslab.hbaseInterfaces.ExtendedHTable;
import pt.uminho.haslab.safeclient.secureTable.CryptoTable;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smpc.exceptions.InvalidNumberOfBits;

import java.io.IOException;

public class CryptoClient extends DefaultHBaseClient {


    public CryptoClient(String configuration) {
        super(configuration);
    }

    @Override
    public ExtendedHTable createTableInterface(String tableName, TableSchema schema) throws IOException, InvalidNumberOfBits {
        Configuration conf = new Configuration();
        conf.addResource(configuration);
        return new CryptoTable(conf, tableName, schema);
    }


}

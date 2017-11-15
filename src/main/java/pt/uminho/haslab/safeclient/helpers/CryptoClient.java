package pt.uminho.haslab.safeclient.helpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import pt.uminho.haslab.safeclient.ExtendedHTable;
import pt.uminho.haslab.safeclient.ExtendedHTableImpl;
import pt.uminho.haslab.safeclient.secureTable.CryptoTable;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.testingutils.ShareCluster;

import java.io.IOException;

public class CryptoClient extends DefaultHBaseClient {


    public CryptoClient(String configuration) throws IOException {
        super(configuration);
    }

    @Override
    public ExtendedHTable createTableInterface(String tableName, TableSchema schema) throws IOException, InvalidNumberOfBits {
        Configuration conf = new Configuration();
        conf.addResource(configuration);
        return new CryptoTable(conf, tableName, schema);
    }


}

package pt.uminho.haslab.safeclient.helpers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import pt.uminho.haslab.hbaseInterfaces.ExtendedHTable;
import pt.uminho.haslab.safeclient.ExtendedHTableImpl;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smpc.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.testingutils.ShareCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Default HBase client used to validate the semantic of the operations
 */
public class DefaultHBaseClient implements AdminProxy {

    protected final String configuration;
    private ShareCluster clusters;
    private HBaseAdmin admin;

    public DefaultHBaseClient(String configuration) {
        clusters = null;
        this.configuration = configuration;

    }

    @Override
    public void createTable(HTableDescriptor tableDescriptor) throws IOException{
        admin.createTable(tableDescriptor);
    }

    @Override
    public void initalizeAdminConnection() throws IOException {
        Configuration conf = new Configuration();
        conf.addResource(configuration);
        admin = new HBaseAdmin(conf);
    }

    @Override
    public void deleteTable(HTableDescriptor tableDescriptor) throws IOException {
        admin.deleteTable(tableDescriptor.getName());
    }

    @Override
    public void disableTable(String tableName) throws IOException {
        admin.disableTable(tableName);
    }

    @Override
    public boolean tableExists(String tableName) throws IOException {
        return admin.tableExists(tableName);
    }

    @Override
    public void close() throws IOException {
        admin.close();
    }

    @Override
    public void startCluster(String tableSchemaPath) throws Exception {
        /*
         * The share cluster can be used with a single HBase instance and works
         * as a default HBase minicluster.
         * tableSchemaPath is not being used. Currently only used on ShareClient
         */
        List<String> resources = new ArrayList<String>();
        resources.add("def-hbase-site.xml");
        clusters = new ShareCluster(resources, 1);

    }

    public void stopCluster() throws IOException {
        clusters.tearDown();
    }

    @Override
    public ExtendedHTable createTableInterface(String tableName, TableSchema schema) throws IOException, InvalidNumberOfBits {
        Configuration conf = new Configuration();
        conf.addResource(configuration);
        return new ExtendedHTableImpl(conf, tableName);
    }


}

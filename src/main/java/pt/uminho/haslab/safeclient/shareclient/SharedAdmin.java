package pt.uminho.haslab.safeclient.shareclient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import pt.uminho.haslab.hbaseInterfaces.CHBaseAdmin;
import pt.uminho.haslab.safeclient.Database;
import pt.uminho.haslab.safemapper.TableSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SharedAdmin implements CHBaseAdmin {

    static final Log LOG = LogFactory.getLog(SharedAdmin.class.getName());

    private final List<HBaseAdmin> admins;
    private final List<Configuration> confs;
    private SharedClientConfiguration sharedConfig;
    private Configuration config;
    private boolean isCreated;
    private TableSchema schema;

    public SharedAdmin(Configuration conf) throws IOException {
        admins = new ArrayList<HBaseAdmin>();
        confs = new ArrayList<Configuration>();
        this.config = conf;
    }

    private synchronized  void initConnections(String tableName) throws IOException {
        if(LOG.isDebugEnabled()){
         LOG.debug("Going to init connection for table " + tableName);
        }
        if(admins.isEmpty()){
            if(LOG.isDebugEnabled()){
                LOG.debug("admin connection are empty" + tableName);
            }
            TableSchema schema = Database.getTableSchema(config, tableName);
            boolean requiresSharedTable = Database.requiresSharedTable(schema);
            if(requiresSharedTable){
                if(LOG.isDebugEnabled()){
                    LOG.debug("connecting shared admins" + tableName);
                }
                for (int i = 1; i < 4; i++) {
                    /**
                     *
                     * This is not the best approach to handle the configurations.
                     */
                    sharedConfig = new SharedClientConfiguration(config, i);
                    Configuration clusterConfig = sharedConfig
                            .createClusterConfiguration();
                    confs.add(clusterConfig);
                    HBaseAdmin admin = new HBaseAdmin(clusterConfig);
                    admins.add(admin);
                }
            }else{

                    initSingleConnection();
            }
        }
    }
    private synchronized  void initSingleConnection() throws IOException {
        if(admins.isEmpty()){
            if(LOG.isDebugEnabled()){
                LOG.debug("connecting single admin");
            }
            sharedConfig = new SharedClientConfiguration(config, 1);
            Configuration clusterConfig = sharedConfig
                    .createClusterConfiguration();
            confs.add(clusterConfig);
            HBaseAdmin admin = new HBaseAdmin(clusterConfig);
            admins.add(admin);
        }
    }

    public void createTable(final HTableDescriptor descriptor)
            throws IOException {
        initConnections(descriptor.getTableName().getNameAsString());
        TableSchema schema = Database.getTableSchema(config, descriptor.getTableName().getNameAsString());
        if (schema.getEncryptionMode()) {
            for (final HBaseAdmin admin : admins) {
                admin.createTable(descriptor);
            }
        } else {
            admins.get(0).createTable(descriptor);
        }
    }

    @Override
    public void createTable(HTableDescriptor descriptor, byte[][] bytes) throws IOException {
        initConnections(descriptor.getTableName().getNameAsString());
        TableSchema schema = Database.getTableSchema(config, descriptor.getTableName().getNameAsString());
        if (schema.getEncryptionMode()) {
            for (final HBaseAdmin admin : admins) {
                admin.createTable(descriptor, bytes);
            }
        } else {
            admins.get(0).createTable(descriptor, bytes);

        }

    }

    public void deleteTable(String tableName) throws IOException {
        initConnections(tableName);
        TableSchema schema = Database.getTableSchema(config, tableName);
        if (schema.getEncryptionMode()) {

            for (HBaseAdmin admin : admins) {
                admin.deleteTable(tableName);
            }
        } else {
            admins.get(0).deleteTable(tableName);
        }

    }

    public void disableTable(String tableName) throws IOException {
        initConnections(tableName);
        TableSchema schema = Database.getTableSchema(config, tableName);
        if (schema.getEncryptionMode()) {
            for (HBaseAdmin admin : admins) {
                admin.disableTable(tableName);
            }
        } else {
            admins.get(0).disableTable(tableName);
        }

    }

    public boolean tableExists(String tableName) throws IOException {
        initConnections(tableName);
        return admins.get(0).tableExists(tableName);

    }

    @Override
    public boolean isTableAvailable(String s) throws IOException {
        initConnections(s);
        return admins.get(0).isTableAvailable(s);
    }

    @Override
    public void createTableAsync(HTableDescriptor hTableDescriptor, byte[][] bytes) throws IOException {
        initConnections(hTableDescriptor.getTableName().getNameAsString());
        TableSchema schema = Database.getTableSchema(config, hTableDescriptor.getTableName().getNameAsString());
        if (schema.getEncryptionMode()) {
            for (HBaseAdmin admin : admins) {
                admin.createTable(hTableDescriptor, bytes);
            }
        } else {
            admins.get(0).createTable(hTableDescriptor, bytes);
        }

    }

    @Override
    public ClusterStatus getClusterStatus() throws IOException {
        initSingleConnection();
        return admins.get(0).getClusterStatus();
    }


    @Override
    public TableName[] listTableNames() throws IOException {
        initSingleConnection();
        return admins.get(0).listTableNames();
    }

    @Override
    public List<HRegionInfo> getTableRegions(TableName tableName) throws IOException {
        initConnections(tableName.getNameAsString());
        return admins.get(0).getTableRegions(tableName);
    }

    public void close() throws IOException {
        initSingleConnection();
        for (HBaseAdmin admin : admins) {
            admin.close();
        }
    }

}

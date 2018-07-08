package pt.uminho.haslab.safeclient.shareclient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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

    public SharedAdmin(Configuration conf) throws IOException {
        admins = new ArrayList<HBaseAdmin>();
        confs = new ArrayList<Configuration>();
        this.config = conf;
        for (int i = 1; i < 4; i++) {
            /**
             *
             * This is not the best approach to handle the configurations.
             */
            sharedConfig = new SharedClientConfiguration(conf, i);
            Configuration clusterConfig = sharedConfig
                    .createClusterConfiguration();
            confs.add(clusterConfig);
            HBaseAdmin admin = new HBaseAdmin(clusterConfig);
            admins.add(admin);
        }

    }

    public void createTable(final HTableDescriptor descriptor)
            throws IOException {
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
        return admins.get(0).tableExists(tableName);

    }

    @Override
    public boolean isTableAvailable(String s) throws IOException {
        return admins.get(0).isTableAvailable(s);
    }

    @Override
    public void createTableAsync(HTableDescriptor hTableDescriptor, byte[][] bytes) throws IOException {
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
        return admins.get(0).getClusterStatus();
    }


    @Override
    public TableName[] listTableNames() throws IOException {
        return admins.get(0).listTableNames();
    }

    @Override
    public List<HRegionInfo> getTableRegions(TableName tableName) throws IOException {
        return admins.get(0).getTableRegions(tableName);
    }

    public void close() throws IOException {

        for (HBaseAdmin admin : admins) {
            admin.close();
        }
    }

}

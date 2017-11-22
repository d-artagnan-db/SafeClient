package pt.uminho.haslab.safeclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class DefaultHBaseAdmin implements CHBaseAdmin {

    private Configuration conf;
    private HBaseAdmin admin;

    public DefaultHBaseAdmin(Configuration conf) throws IOException {
        System.out.println("Create default HBase admin");
        admin = new HBaseAdmin(conf);
        this.conf = conf;
    }

    @Override
    public void createTable(HTableDescriptor descriptor) throws IOException, InterruptedException {
        this.admin.createTable(descriptor);
    }

    @Override
    public void deleteTable(String tableName) throws IOException {
        this.admin.deleteTable(tableName);
    }

    @Override
    public void disableTable(String tableName) throws IOException {
        this.admin.disableTable(tableName);
    }

    @Override
    public boolean tableExists(String tableName) throws IOException {
        return admin.tableExists(tableName);
    }

    @Override
    public void close() throws IOException {
        admin.close();
    }
}

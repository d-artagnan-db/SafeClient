package pt.uminho.haslab.safeclient.helpers;

import org.apache.hadoop.hbase.HTableDescriptor;
import pt.uminho.haslab.hbaseInterfaces.ExtendedHTable;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smpc.exceptions.InvalidNumberOfBits;

import java.io.IOException;

public interface AdminProxy {

    void startCluster(String tableSchemaPath) throws Exception;

    void stopCluster() throws IOException;

    ExtendedHTable createTableInterface(String tableName, TableSchema schema)
            throws IOException, InvalidNumberOfBits, InvalidNumberOfBits;

    void createTable(final HTableDescriptor tableDescriptor) throws IOException, InterruptedException;

    void initalizeAdminConnection() throws IOException;

    void deleteTable(final HTableDescriptor tableDescriptor) throws IOException;

    void disableTable(String tableName) throws IOException;

    boolean tableExists(String tableName) throws IOException;

    void close() throws IOException;


}

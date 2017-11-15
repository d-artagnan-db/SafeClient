package pt.uminho.haslab.safeclient.shareclient.benchmarks;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import pt.uminho.haslab.safemapper.TableSchema;

public interface BenchClient {
    // Start and Stop Cluster are only used for local tests
    void startCluster() throws Exception;

    void stopCluster() throws Exception;

    void createTable(HTableDescriptor table) throws Exception;

    void deleteTable(String tableName) throws Exception;

    void closeClientConnection() throws Exception;

    HTableInterface getTableInterface(String tableName, TableSchema schema) throws Exception;

}

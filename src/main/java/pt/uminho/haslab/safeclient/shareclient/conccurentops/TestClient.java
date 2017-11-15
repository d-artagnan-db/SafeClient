package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import pt.uminho.haslab.safemapper.TableSchema;

import java.io.IOException;

public interface TestClient {

    void createTestTable(HTableDescriptor testTable) throws Exception;

    HTableInterface createTableInterface(String tableName, TableSchema schema)
            throws Exception;

    boolean checkTableExists(String tableName) throws Exception;

    void startCluster() throws Exception;

    void stopCluster() throws IOException;
}

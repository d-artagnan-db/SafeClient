package pt.uminho.haslab.safecloudclient.shareclient.conccurentops;

import java.io.IOException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;

public interface TestClient {

	public void createTestTable(HTableDescriptor testTable) throws Exception;

	public abstract HTableInterface createTableInterface(String tableName)
			throws Exception;

	public boolean checkTableExists(String tableName) throws Exception;

	public void startCluster() throws Exception;

	public void stopCluster() throws IOException;
}

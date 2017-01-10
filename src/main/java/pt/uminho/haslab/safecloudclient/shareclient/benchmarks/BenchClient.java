package pt.uminho.haslab.safecloudclient.shareclient.benchmarks;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;

public interface BenchClient {
	// Start and Stop Cluster are only used for local tests
	public abstract void startCluster() throws Exception;

	public abstract void stopCluster() throws Exception;

	public abstract void createTable(HTableDescriptor table) throws Exception;

	public abstract void deleteTable(String tableName) throws Exception;

	public abstract void closeClientConnection() throws Exception;

	public HTableInterface getTableInterface(String tableName) throws Exception;

}

package pt.uminho.haslab.safeclient.shareclient.benchmarks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

import java.io.IOException;

public class BaselineClient implements BenchClient {

	private final HBaseAdmin admin;
	private final Configuration conf;
	public BaselineClient(String configurationFilePath)
			throws ZooKeeperConnectionException, IOException {
		conf = new Configuration();
		conf.addResource(configurationFilePath);
		admin = new HBaseAdmin(conf);
	}

	@Override
	public void createTable(HTableDescriptor table) throws IOException {
		admin.createTable(table);

	}

	@Override
	public void deleteTable(String tableName) throws IOException {
		// For a table to be deleted it must be first disabled
		admin.disableTable(tableName);
		admin.deleteTable(tableName);

	}

	@Override
    public HTableInterface getTableInterface(String tableName, TableSchema schema)
            throws IOException, InvalidNumberOfBits {
		return new HTable(conf, tableName);

	}

	@Override
	public void closeClientConnection() throws IOException {
		admin.close();
	}

	@Override
	public void startCluster() {
	}

	@Override
	public void stopCluster() throws Exception {
	}

}

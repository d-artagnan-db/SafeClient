package pt.uminho.haslab.safeclient.shareclient.benchmarks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import pt.uminho.haslab.safeclient.shareclient.SharedAdmin;
import pt.uminho.haslab.safeclient.shareclient.SharedTable;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

import java.io.IOException;

public class ShareBench implements BenchClient {

	private final SharedAdmin admin;
	private final Configuration conf;

	public ShareBench(String configurationFilePath) throws IOException {
		conf = new Configuration();
		conf.addResource(configurationFilePath);
		admin = new SharedAdmin(conf);
	}

	@Override
	public void createTable(HTableDescriptor table) throws IOException,
			InterruptedException {
		admin.createTable(table);
	}

	@Override
	public void deleteTable(String tableName) throws IOException {
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
	}

	@Override
    public HTableInterface getTableInterface(String tableName, TableSchema schema)
            throws IOException, InvalidNumberOfBits {
        return new SharedTable(conf, tableName, schema);
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

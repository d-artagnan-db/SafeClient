package pt.uminho.haslab.safecloudclient.clients;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoTable;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.testingutils.ShareCluster;

/**
 * Default HBase client used to validate the semantic of the operations
 */
public class DefaultHBaseClient implements TestClient {

	private ShareCluster clusters;
	private final HBaseAdmin admin;

	public DefaultHBaseClient() throws ZooKeeperConnectionException,
			IOException {
		clusters = null;
		Configuration conf = new Configuration();
		conf.addResource("def-hbase-client.xml");
		admin = new HBaseAdmin(conf);
	}

	public String getTableName() {
		return null;
	}

	public void createTestTable(HTableDescriptor testTable)
			throws ZooKeeperConnectionException, IOException {
		admin.createTable(testTable);
	}

	public HTableInterface createTableInterface(String tableName)
			throws IOException, InvalidNumberOfBits {
		Configuration conf = new Configuration();
		conf.addResource("def-hbase-client.xml");
		CryptoTable ct = new CryptoTable(conf, tableName, "schema.txt");
		return ct;
	}

	public boolean checkTableExists(String tableName) throws IOException {
		return admin.tableExists(tableName);
	}

	public void startCluster() throws Exception {
		List<String> resources = new ArrayList<String>();
		resources.add("def-hbase-site.xml");
		/**
		 * The share cluster can be used with a single HBase instance and works
		 * as a default HBase minicluster.
		 * 
		 */
		clusters = new ShareCluster(resources);
	}

	public void stopCluster() throws IOException {
		clusters.tearDown();
	}

}

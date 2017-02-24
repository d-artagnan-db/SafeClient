package pt.uminho.haslab.safecloudclient.clients;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.After;
import pt.uminho.haslab.safecloudclient.shareclient.SharedAdmin;
import pt.uminho.haslab.safecloudclient.shareclient.SharedTable;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.testingutils.ShareCluster;

public class ShareClient implements TestClient {

	private ShareCluster clusters;
	private SharedAdmin admin;

	public ShareClient() throws Exception {
		System.setProperty("hadoop.home.dir", "/home/roger/hadoop-2.7.2");
		// inicialized on start method
		clusters = null;
		admin = null;

	}

	public void createTestTable(HTableDescriptor testeTable)
			throws IOException, InterruptedException {
		admin.createTable(testeTable);
	}

	public boolean checkTableExists(String tableName) throws IOException {
		return admin.tableExits(tableName);
	}

	@After
	public void tearDown() throws IOException {
		clusters.tearDown();
	}

	public HTableInterface createTableInterface(String tableName)
			throws IOException, InvalidNumberOfBits {
		Configuration conf = new Configuration();
		conf.addResource("hbase-client.xml");
		return new SharedTable(conf, tableName);
	}

	public void startCluster() throws Exception {
		List<String> resources = new ArrayList<String>();

		for (int i = 0; i < 3; i++) {
			resources.add("hbase-site-" + i + ".xml");
		}
		// Boot the clusters
		clusters = new ShareCluster(resources);
		Configuration conf = new Configuration();
		conf.addResource("hbase-client.xml");
		admin = new SharedAdmin(conf);
	}

	public void stopCluster() throws IOException {
		clusters.tearDown();
	}

}

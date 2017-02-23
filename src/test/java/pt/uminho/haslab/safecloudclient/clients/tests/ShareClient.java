package pt.uminho.haslab.safecloudclient.clients.tests;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.junit.After;
import pt.uminho.haslab.safecloudclient.shareclient.SharedAdmin;
import pt.uminho.haslab.safecloudclient.shareclient.SharedTable;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.testingutils.ShareCluster;
import org.apache.commons.logging.Log;

public class ShareClient implements TestClient {
    static final Log LOG = LogFactory.getLog(ShareClient.class.getName());

	private ShareCluster clusters;
	private SharedAdmin admin;

	public ShareClient() {
		System.out.println("Going to start Shareclient ");
		System.setProperty("hadoop.home.dir", "/");

		clusters = null;
		admin = null;

	}

	@Override
	public void createTestTable(HTableDescriptor testeTable)
			throws IOException, InterruptedException {
		LOG.debug("Going to createTestTable");
		admin.createTable(testeTable);
		LOG.debug("Test table created");

	}

	@Override
	public boolean checkTableExists(String tableName) throws IOException {
		return admin.tableExits(tableName);
	}

	@After
	public void tearDown() throws IOException {
		clusters.tearDown();
	}

	@Override
	public HTableInterface createTableInterface(String tableName)
			throws IOException, InvalidNumberOfBits {
		Configuration conf = new Configuration();
		conf.addResource("hbase-client.xml");
		return new SharedTable(conf, tableName);
	}

	@Override
	public void startCluster() throws Exception {
		List<String> resources = new ArrayList<String>();

		for (int i = 0; i < 3; i++) {
			resources.add("hbase-site-" + i + ".xml");
		}
		// Boot the clusters
		clusters = new ShareCluster(resources);
		Thread.sleep(30000);
		Configuration conf = new Configuration();
		conf.addResource("hbase-client.xml");
		admin = new SharedAdmin(conf);
	}

	@Override
	public void stopCluster() throws IOException {
		clusters.tearDown();
	}

}

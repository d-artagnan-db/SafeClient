package pt.uminho.haslab.safeclient.helpers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.junit.After;
import pt.uminho.haslab.safeclient.ExtendedHTable;
import pt.uminho.haslab.safeclient.shareclient.ResultPlayerLoadBalancerImpl;
import pt.uminho.haslab.safeclient.shareclient.SharedAdmin;
import pt.uminho.haslab.safeclient.shareclient.SharedTable;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.testingutils.ShareCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShareClient implements AdminProxy {

	static final Log LOG = LogFactory.getLog(ShareClient.class.getName());

	private ShareCluster clusters;
	private SharedAdmin admin;
	protected final String configuration;

	public ShareClient(String configuration) throws IOException {
		LOG.debug("Going to start Shareclient ");
		SharedTable.initializeLoadBalancer(new ResultPlayerLoadBalancerImpl());
		clusters = null;
		this.configuration = configuration;
	}

	@Override
	public void createTable(HTableDescriptor testeTable) throws IOException, InterruptedException {
		LOG.debug("Going to createTestTable");
		admin.createTable(testeTable);
		LOG.debug("Test table created");

	}

	@Override
	public void initalizeAdminConnection() throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(configuration);
		admin = new SharedAdmin(conf);
	}

	@Override
	public void deleteTable(HTableDescriptor tableDescriptor) throws IOException {
		admin.deleteTable(tableDescriptor.getNameAsString());
	}

	@Override
	public void disableTable(String tableName) throws IOException {
		admin.deleteTable(tableName);

	}

	@Override
	public boolean tableExists(String tableName) throws IOException {
		return admin.tableExits(tableName);
	}

	@Override
	public void close() throws IOException {
		admin.close();
	}

	@After
	public void tearDown() throws IOException {
		clusters.tearDown();
	}

	@Override
	public ExtendedHTable createTableInterface(String tableName, TableSchema schema)
			throws IOException, InvalidNumberOfBits {
		Configuration conf = new Configuration();
		conf.addResource(configuration);
		return new SharedTable(conf, tableName, schema);
	}

	@Override
	public void startCluster(String tableSchemaPath) throws Exception {
		List<String> resources = new ArrayList<String>();

		for (int i = 0; i < 3; i++) {
			resources.add("hbase-site-" + i + ".xml");
		}
		// Boot the clusters
		clusters = new ShareCluster(resources, 1, tableSchemaPath);
	}

	@Override
	public void stopCluster() throws IOException {
		clusters.tearDown();
	}

}

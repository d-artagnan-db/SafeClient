package pt.uminho.haslab.safeclient.shareclient.benchmarks;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTableInterface;
import pt.uminho.haslab.safeclient.shareclient.SharedAdmin;
import pt.uminho.haslab.safeclient.shareclient.SharedTable;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.testingutils.ShareCluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ShareBenchTest implements BenchClient {

	private ShareCluster clusters;
	private SharedAdmin admin;

	public ShareBenchTest() throws Exception {
		clusters = null;
		admin = null;
	}

	@Override
	public void createTable(HTableDescriptor table) throws IOException,
			InterruptedException {
		admin.createTable(table);
	}

	@Override
	public void deleteTable(String tableName) throws IOException {
		admin.deleteTable(tableName);
	}

	@Override
    public HTableInterface getTableInterface(String tableName, TableSchema schema)
            throws IOException, InvalidNumberOfBits {
        Configuration conf = new Configuration();
		conf.addResource("hbase-client.xml");
        return new SharedTable(conf, tableName, schema);
    }

	@Override
	public void closeClientConnection() throws IOException {
		admin.close();
	}

	@Override
	public void startCluster() throws Exception {
		List<String> resources = new ArrayList<String>();
		System.out.println("going to start cluster");

		for (int i = 1; i < 4; i++) {

			resources.add("hbase-site-" + i + ".xml");
		}
		System.out.println("Resources added");
		// Boot the clusters
		clusters = new ShareCluster(resources,1);
		Configuration conf = new Configuration();
		conf.addResource("hbase-client.xml");
		admin = new SharedAdmin(conf);
		System.out.println("Cluster started");
	}

	@Override
	public void stopCluster() throws Exception {
		admin.close();
		clusters.tearDown();
	}

}

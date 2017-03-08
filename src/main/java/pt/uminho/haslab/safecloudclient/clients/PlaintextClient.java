package pt.uminho.haslab.safecloudclient.clients;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

import java.io.IOException;

public class PlaintextClient implements TestClient {

	private final HBaseAdmin admin;

	public PlaintextClient() throws ZooKeeperConnectionException, IOException {
		Configuration conf = new Configuration();
		conf.addResource("conf.xml");
		admin = new HBaseAdmin(conf);
	}

	public void createTestTable(HTableDescriptor testTable)
			throws ZooKeeperConnectionException, IOException {
		admin.createTable(testTable);
	}

	public HTableInterface createTableInterface(String tableName)
			throws IOException, InvalidNumberOfBits {
		Configuration conf = new Configuration();
		conf.addResource("conf.xml");

		System.out.println("Table created Successfully");
		return new HTable(conf, tableName);
	}

	public boolean checkTableExists(String tableName) throws IOException {
		return admin.tableExists(tableName);
	}

	public void startCluster() throws Exception {
		System.out.println("Started CLuster");
	}

	public void stopCluster() throws IOException {
		System.out.println("Stoped CLuster");
	}

}

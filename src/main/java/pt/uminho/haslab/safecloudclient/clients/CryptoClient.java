package pt.uminho.haslab.safecloudclient.clients;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoTable;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.testingutils.ShareCluster;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Created by rgmacedo on 3/1/17.
 */
public class CryptoClient implements TestClient {

	private final HBaseAdmin admin;
	private final String tablename;
	private final CryptoTechnique.CryptoType cryptoType;

	public CryptoClient(String tName, CryptoTechnique.CryptoType cType)
			throws ZooKeeperConnectionException, IOException {
		Configuration conf = new Configuration();
		conf.addResource("conf.xml");
		admin = new HBaseAdmin(conf);
		tablename = tName;
		cryptoType = cType;
	}

	public String getTableName() {
		return this.tablename;
	}

	public void createTestTable(HTableDescriptor testTable)
			throws ZooKeeperConnectionException, IOException {
		admin.createTable(testTable);
	}

	public HTableInterface createTableInterface(String tableName)
			throws IOException, InvalidNumberOfBits {
		Configuration conf = new Configuration();
		conf.addResource("conf.xml");

		CryptoTable ct = new CryptoTable(conf, this.tablename, this.cryptoType);
		byte[] key = CryptoProperties.readKeyFromFile("key.txt");
		ct.cryptoProperties.setKey(key);

		System.out.println("Table created Successfully");
		return ct;
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

package pt.uminho.haslab.safecloudclient.clients;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import pt.uminho.haslab.cryptoenv.CryptoTechnique.CryptoType;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoTable;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import java.io.IOException;

public class CryptoClient implements TestClient {

	private final HBaseAdmin admin;
	private final CryptoType cryptoType;

	public CryptoClient(CryptoType cType) throws ZooKeeperConnectionException,
			IOException {
		Configuration conf = new Configuration();
		conf.addResource("conf.xml");
		admin = new HBaseAdmin(conf);
		cryptoType = cType;
	}

	public void createTestTable(HTableDescriptor testTable)
			throws ZooKeeperConnectionException, IOException {
		admin.createTable(testTable);
	}

	public HTableInterface createTableInterface(String tableName)
			throws IOException, InvalidNumberOfBits {
		Configuration conf = new Configuration();
		conf.addResource("conf.xml");

		CryptoTable ct = new CryptoTable(conf, tableName, this.cryptoType);
		byte[] key = CryptoProperties.readKeyFromFile("key.txt");

		ct.cryptoProperties.setKey(CryptoType.STD, key);
		ct.cryptoProperties.setKey(CryptoType.DET, key);
		ct.cryptoProperties.setKey(CryptoType.OPE, key);

		System.out.println("Table created Successfully");
		return ct;
	}

	public boolean checkTableExists(String tableName) throws IOException {
		return admin.tableExists(tableName);
	}

	public void startCluster() throws Exception {
		System.out.println("Started Cluster");
	}

	public void stopCluster() throws IOException {
		System.out.println("Stoped Cluster");
	}
}

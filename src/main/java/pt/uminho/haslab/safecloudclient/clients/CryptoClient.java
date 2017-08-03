package pt.uminho.haslab.safecloudclient.clients;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import pt.uminho.haslab.cryptoenv.CryptoTechnique.CryptoType;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoTable;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

import java.io.IOException;

public class CryptoClient implements TestClient {

	private final HBaseAdmin admin;
	private String schemaFile;

	public CryptoClient(String schemaFileName)
			throws ZooKeeperConnectionException, IOException {
		Configuration conf = new Configuration();
		conf.addResource("conf.xml");
		admin = new HBaseAdmin(conf);
		schemaFile = schemaFileName;
	}

	public void createTestTable(HTableDescriptor testTable)
			throws ZooKeeperConnectionException, IOException {
		admin.createTable(testTable);
	}

	public HTableInterface createTableInterface(String tableName)
			throws IOException, InvalidNumberOfBits {
		Configuration conf = new Configuration();
		conf.addResource("conf.xml");


		new Thread(new CreateTable("temp_table1")).start();
		new Thread(new CreateTable("temp_table2")).start();
		new Thread(new CreateTable("temp_table3")).start();

		CryptoTable ct = new CryptoTable(conf, tableName);
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


	class CreateTable implements Runnable {
		String tableName;

		public CreateTable(String tableName) {
			this.tableName = tableName;
		}

		@Override
		public void run() {
			Configuration conf = new Configuration();
			conf.addResource("conf.xml");


			CryptoTable ct = null;
			try {
				ct = new CryptoTable(conf, tableName);
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

}

package pt.uminho.haslab.safecloudclient.clients;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoTable;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;

import java.io.IOException;

/**
 * Created by rgmacedo on 5/10/17.
 */
public class LeanXScaleClient implements TestClient {

    private final HBaseAdmin admin;

    public LeanXScaleClient()
            throws ZooKeeperConnectionException, IOException {
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

        CryptoTable ct = new CryptoTable(conf, tableName);
//        byte[] key = Utils.readKeyFromFile("key.txt");
//
//        ct.cryptoProperties.setKey(CryptoTechnique.CryptoType.STD, key);
//        ct.cryptoProperties.setKey(CryptoTechnique.CryptoType.DET, key);
//        ct.cryptoProperties.setKey(CryptoTechnique.CryptoType.OPE, key);
//        ct.cryptoProperties.setKey(CryptoTechnique.CryptoType.FPE, key);

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

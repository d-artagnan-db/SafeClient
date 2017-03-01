package pt.uminho.haslab.safecloudclient.clients;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
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

        public CryptoClient(String tName, CryptoTechnique.CryptoType cType) throws ZooKeeperConnectionException, IOException {
            Configuration conf = new Configuration();
            conf.addResource("conf.xml");
            admin = new HBaseAdmin(conf);
            tablename = tName;
            cryptoType = cType;
        }

        public String getTableName() {
            return this.tablename;
        }

        public void createTestTable(HTableDescriptor testTable) throws ZooKeeperConnectionException, IOException {
            admin.createTable(testTable);
        }

        public HTableInterface createTableInterface(String tableName) throws IOException, InvalidNumberOfBits {
            Configuration conf = new Configuration();
            conf.addResource("conf.xml");

            CryptoTable ct = new CryptoTable(conf, this.tablename, this.cryptoType);
            ct.cryptoProperties.setKey(readKeyFromFile("key.txt"));

            return ct;
        }

        public boolean checkTableExists(String tableName) throws IOException {
            return admin.tableExists(tableName);
        }

        public void startCluster() throws Exception {
            System.out.println("Started CLuster");
//            List<String> resources = new ArrayList<String>();
//            resources.add("conf.xml");
//            /**
//             * The share cluster can be used with a single HBase instance and works
//             * as a default HBase minicluster.
//             *
//             */
//            clusters = new ShareCluster(resources);
        }

        public void stopCluster() throws IOException {
//            clusters.tearDown();

            System.out.println("Stoped CLuster");
        }

        public byte[] readKeyFromFile(String filename) throws IOException {
            FileInputStream stream = new FileInputStream(filename);
            try {

                byte[] key = new byte[stream.available()];
                int b;
                int i = 0;
                while ((b = stream.read()) != -1) {
                    key[i] = (byte) b;
                    i++;
                }
                System.out.println("readKeyFromFile: " + Arrays.toString(key));

                return key;
            } catch (Exception e) {
                System.out.println("Exception. " + e.getMessage());
            } finally {
                stream.close();
            }
            return null;
        }


}

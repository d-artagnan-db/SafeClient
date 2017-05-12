package pt.uminho.haslab.safecloudclient.tests;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.clients.TestClient;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

/**
 * Created by rgmacedo on 5/10/17.
 */
public class QEngineHBaseFeaturesTest extends QEngineTest {

    public Utils utils;
    public List<String> families;
    public String[] qualifiers;
    public Random random;
    HBaseAdmin admin;

    public QEngineHBaseFeaturesTest(int maxBits, List<BigInteger> values) throws Exception {
        super(maxBits, values);
        this.utils = new Utils();
        this.qualifiers = new String[]{"qal1","qal2","qal3","qal4","qal5","qal6","qal7","qal8","qal9"};
        this.families = new ArrayList<>();
        this.random = new Random(1024);

        Configuration conf = new Configuration();
        conf.addResource("conf.xml");
        this.admin = new HBaseAdmin(conf);
    }

    @Override
    protected void testExecution(TestClient client, String tableName) throws Exception {
        HTableInterface table;
        int time = 10000;
        table = client.createTableInterface(tableName);
        LOG.debug("Test Execution [" + tableName + "]\n");
        System.out.println("Table execution " + tableName);

        getColumnFamilies(tableName);

        testPut(table, 10);
    }

    public void testPut(HTableInterface table, int totalOperations) {
        for(int i = 0; i < totalOperations; i++) {
            Put p = new Put(String.valueOf(i).getBytes());
            for(String family : this.families) {
                p.add(family.getBytes(), chooseRandomQualifier().getBytes(), buildRandomValue().getBytes());
            }
            try {
                System.out.println("Put<"+i+","+p.toString()+">");
                table.put(p);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void testGet(HTableInterface table, int totalOperations) {

    }

    public void getColumnFamilies(String tableName) throws IOException {
        HTableDescriptor tableDescriptor = admin.getTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor[] hColumnDescriptors = tableDescriptor.getColumnFamilies();
        for(int i = 0; i < hColumnDescriptors.length; i++) {
            System.out.println("Column Descriptor: "+hColumnDescriptors[i].toString());
            this.families.add(hColumnDescriptors[i].getNameAsString());
        }
    }

    public String chooseRandomQualifier() {
        return this.qualifiers[this.random.nextInt(qualifiers.length-1)];
    }

    public String buildRandomValue() {
        return String.valueOf(this.random.nextInt(500000));
    }
}

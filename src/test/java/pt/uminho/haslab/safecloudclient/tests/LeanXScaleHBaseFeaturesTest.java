package pt.uminho.haslab.safecloudclient.tests;

import org.apache.hadoop.hbase.client.HTableInterface;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.clients.TestClient;

import java.math.BigInteger;
import java.util.*;

/**
 * Created by rgmacedo on 5/10/17.
 */
public class LeanXScaleHBaseFeaturesTest extends LeanXScaleTest {

    public Utils utils;
    public Map<Integer,byte[]> sharedMap;

    public LeanXScaleHBaseFeaturesTest(int maxBits, List<BigInteger> values) throws Exception {
        super(maxBits, values);
        this.utils = new Utils();
        this.sharedMap = new HashMap<>();
    }

    @Override
    protected void testExecution(TestClient client, String tableName) throws Exception {
        HTableInterface table;
        int time = 10000;
        table = client.createTableInterface(tableName);
        LOG.debug("Test Execution [" + tableName + "]\n");
        System.out.println("Table execution " + tableName);


    }


}

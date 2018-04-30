package pt.uminho.haslab.safeclient.tests.ConcurrentTests;

import org.junit.Before;
import pt.uminho.haslab.safeclient.helpers.AdminProxy;
import pt.uminho.haslab.safeclient.helpers.RedisUtils;
import pt.uminho.haslab.safeclient.helpers.ShareClient;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.smpc.helpers.RandomGenerator;

import java.io.IOException;
import java.util.HashMap;

public class IMPCConcurrentFilterTest extends ConcurrentFilters {

    @Before
    public void initializeRedisContainer() throws IOException {
        RedisUtils.flushAll("localhost");
        RandomGenerator.initIntBatch(100);
    }

    @Override
    protected DatabaseSchema.CryptoType getProtectedColumnType() {
        return DatabaseSchema.CryptoType.ISMPC;
    }

    @Override
    protected int getProtectedColumnFormatSize() {
        return 0;
    }

    @Override
    protected AdminProxy getProtectedHBaseAdmin() throws Exception {
        return new ShareClient("share-hbase-client.xml");
    }

    protected void defineColTypes() {
        this.qualifierColTypes.put("Teste", new HashMap<String, ColType>());
        this.qualifierColTypes.get("Teste").put("Name", ColType.STRING);
        this.qualifierColTypes.get("Teste").put("Age", ColType.INTEGER);
    }

    @Override
    protected String getProtectedSchemaPath() {
        return "/Users/roger/Documents/HASLab/safecloud-eu/safeclient/src/test/resources/IMPC-put-get-schema.xml";
    }
}

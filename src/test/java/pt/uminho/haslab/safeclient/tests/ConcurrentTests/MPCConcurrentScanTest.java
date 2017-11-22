package pt.uminho.haslab.safeclient.tests.ConcurrentTests;

import org.junit.Before;
import pt.uminho.haslab.safeclient.helpers.AdminProxy;
import pt.uminho.haslab.safeclient.helpers.RedisUtils;
import pt.uminho.haslab.safeclient.helpers.ShareClient;
import pt.uminho.haslab.safemapper.DatabaseSchema;

import java.io.IOException;

public class MPCConcurrentScanTest extends ConcurrentScans {

    @Before
    public void initializeRedisContainer() throws IOException {
        RedisUtils.flushAll("redis");
    }

    protected DatabaseSchema.CryptoType getProtectedColumnType() {
        return DatabaseSchema.CryptoType.SMPC;
    }

    protected AdminProxy getProtectedHBaseAdmin() throws Exception {
        return new ShareClient("share-hbase-client.xml");
    }

    protected String getProtectedSchemaPath() {
        return "put-get-schema.xml";
    }

    protected int getProtectedColumnFormatSize() {
        return 64;
    }
}

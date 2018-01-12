package pt.uminho.haslab.safeclient.tests.BaseTests;

import org.junit.Before;
import pt.uminho.haslab.safeclient.helpers.AdminProxy;
import pt.uminho.haslab.safeclient.helpers.RedisUtils;
import pt.uminho.haslab.safeclient.helpers.ShareClient;
import pt.uminho.haslab.safemapper.DatabaseSchema;

import java.io.IOException;

public class SimpleMPCTest extends SingleColumnProtectedTest {

    @Before
    public void initializeRedisContainer() throws IOException {
        RedisUtils.flushAll("localhost");
    }

    @Override
    protected DatabaseSchema.CryptoType getProtectedColumnType() {
        return DatabaseSchema.CryptoType.SMPC;
    }

    @Override
    protected AdminProxy getProtectedHBaseAdmin() throws Exception {
        return new ShareClient("share-hbase-client.xml");
    }

    @Override
    protected String getProtectedSchemaPath() {
        return "put-get-schema.xml";
    }
    protected int getProtectedColumnFormatSize(){
        return 64;
    }

}

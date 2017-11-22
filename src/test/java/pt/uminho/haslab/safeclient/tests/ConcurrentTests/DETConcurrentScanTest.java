package pt.uminho.haslab.safeclient.tests.ConcurrentTests;

import pt.uminho.haslab.safeclient.helpers.AdminProxy;
import pt.uminho.haslab.safeclient.helpers.CryptoClient;
import pt.uminho.haslab.safemapper.DatabaseSchema;

public class DETConcurrentScanTest extends ConcurrentScans {

    @Override
    protected DatabaseSchema.CryptoType getProtectedColumnType() {
        return DatabaseSchema.CryptoType.DET;
    }

    @Override
    protected AdminProxy getProtectedHBaseAdmin() throws Exception {
        return new CryptoClient("protected-hbase-client.xml");
    }

    @Override
    protected String getProtectedSchemaPath() {
        return null;
    }

    protected int getProtectedColumnFormatSize() {
        return 4;
    }
}

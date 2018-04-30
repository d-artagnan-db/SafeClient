package pt.uminho.haslab.safeclient.tests.SingleColumnValueFilter;

import org.apache.hadoop.hbase.filter.CompareFilter;
import pt.uminho.haslab.safeclient.helpers.AdminProxy;
import pt.uminho.haslab.safeclient.helpers.ShareClient;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.smpc.helpers.RandomGenerator;

import java.util.HashMap;

public class EqualIMPCTest extends SingleColumnMPCTest {

    public EqualIMPCTest() {
        RandomGenerator.initIntBatch(100);

    }

    @Override
    protected CompareFilter.CompareOp getOperation() {
        return CompareFilter.CompareOp.EQUAL;
    }

    @Override
    protected DatabaseSchema.CryptoType getProtectedColumnType() {
        return DatabaseSchema.CryptoType.ISMPC;
    }

    @Override
    protected AdminProxy getProtectedHBaseAdmin() throws Exception {
        return new ShareClient("share-hbase-client.xml");
    }

    @Override
    protected String getProtectedSchemaPath() {
        return "/Users/roger/Documents/HASLab/safecloud-eu/safeclient/src/test/resources/IMPC-put-get-schema.xml";
    }

    protected void defineColTypes() {
        this.qualifierColTypes.put("Teste", new HashMap<String, ColType>());
        this.qualifierColTypes.get("Teste").put("Name", ColType.STRING);
        this.qualifierColTypes.get("Teste").put("Age", ColType.INTEGER);
    }

    protected int getProtectedColumnFormatSize() {
        return 64;
    }
}

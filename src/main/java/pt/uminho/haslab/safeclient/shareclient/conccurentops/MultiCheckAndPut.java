package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import pt.uminho.haslab.safeclient.shareclient.SharedClientConfiguration;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smpc.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smpc.exceptions.InvalidSecretValue;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public class MultiCheckAndPut extends MultiOP {

    private final byte[] row;
    private final byte[] family;
    private final byte[] qualifier;
    private final byte[] value;

    //Vars for check and put on protected column family:column qualifier
    private final long requestID;
    private final int targetPlayer;
    private boolean isProtectedCheck;
    private boolean protectedCheckResult;


    private List<Put> protectedPut;

    public MultiCheckAndPut(SharedClientConfiguration config, List<HTable> connections, TableSchema schema,
                            byte[] row, byte[] family, byte[] qualifier, byte[] value, long requestID, int targetPlayer,
                            Put put) throws InvalidNumberOfBits, IOException, InvalidSecretValue {

        super(config, connections, schema);
        this.row = row;
        this.family = family;
        this.qualifier = qualifier;
        this.value = value;
        this.requestID = requestID;
        this.targetPlayer = targetPlayer;
        validateCheckAndPut();
        this.protectedPut = generateMPCPut(put);

    }

    private void validateCheckAndPut() {

        String fam = new String(family, Charset.forName("UTF-8"));
        String qual = new String(qualifier, Charset.forName("UTF-8"));

        if (schema.getCryptoTypeFromQualifier(fam, qual) == DatabaseSchema.CryptoType.SMPC) {
            isProtectedCheck = true;

            SingleColumnValueFilter scvf = new SingleColumnValueFilter(family, qualifier, CompareFilter.CompareOp.EQUAL, value);
            Scan s = new Scan();
            s.setFilter(scvf);
            MultiScan ms = new MultiScan(config, connections, schema, requestID, targetPlayer, s);
            try {
                ms.startScan();
                Result r = ms.next();
                if (r != null) {
                    protectedCheckResult = true;
                }
                ms.close();
            } catch (IOException e) {
                LOG.error(e);
                throw new IllegalStateException(e);
            }
        }

    }

    @Override
    protected Thread queryThread(SharedClientConfiguration config, HTable table, int index) throws IOException {
        return new CheckAndPutThread(config, table, protectedPut.get(index), row, family, qualifier, value, isProtectedCheck, protectedCheckResult);
    }

    @Override
    protected void threadsJoined(List<Thread> threads) throws IOException {

        if (!isProtectedCheck) {
            boolean resOne = ((CheckAndPutThread) threads.get(0)).getResultCheckAndPut();
            boolean resTwo = ((CheckAndPutThread) threads.get(1)).getResultCheckAndPut();
            boolean resThree = ((CheckAndPutThread) threads.get(2)).getResultCheckAndPut();
            protectedCheckResult = resOne && resTwo && resThree;
        }
    }

    public boolean getResult() {
        return protectedCheckResult;
    }

    private class CheckAndPutThread extends QueryThread {

        private Put put;
        private byte[] row;
        private byte[] family;
        private byte[] qualifier;
        private byte[] value;
        private boolean isProtectedCheck;
        private boolean protectedCheckResult;
        private boolean resultCheckAndPut;

        public CheckAndPutThread(SharedClientConfiguration conf, HTable table, Put put, byte[] row, byte[] family,
                                 byte[] qualifier, byte[] value, boolean isProtectedCheck, boolean protectedCheckResult) {
            super(conf, table);
            this.put = put;
            this.row = row;
            this.family = family;
            this.qualifier = qualifier;
            this.value = value;
            this.isProtectedCheck = isProtectedCheck;
            this.protectedCheckResult = protectedCheckResult;

        }

        @Override
        protected void query() throws IOException {
            if (isProtectedCheck && protectedCheckResult) {
                table.put(put);
            } else if (!isProtectedCheck) {
                resultCheckAndPut = table.checkAndPut(row, family, qualifier, value, put);
            }
        }

        public boolean getResultCheckAndPut() {
            return resultCheckAndPut;
        }

    }
}

package pt.uminho.haslab.safeclient.shareclient;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Pair;
import pt.uminho.haslab.hbaseInterfaces.ExtendedHTable;
import pt.uminho.haslab.safeclient.shareclient.conccurentops.*;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smpc.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smpc.exceptions.InvalidSecretValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * Current implementation assumes that tables identifiers are not protected with secret sharing and only columns
 * can be protected. Thus, any necessary processing over those columns is only triggered by Filters.
 * This version supports distributed clusters with multiple region servers.
 */
public class SharedTable implements ExtendedHTable {

    static final Log LOG = LogFactory.getLog(SharedTable.class.getName());

    private static final AtomicLong identifierGenerator = new AtomicLong();
    private static final TableLock TABLE_LOCKS = new TableLockImpl();
    private static ResultPlayerLoadBalancer LB = new ResultPlayerLoadBalancerImpl();
    private static ExecutorService threadPool;
    private final List<HTable> connections;
    private final Lock readLock;
    private final Lock writeLock;
    private SharedClientConfiguration sharedConfig;
    private TableSchema schema;


    public SharedTable(Configuration conf, String tableName, TableSchema schema) throws IOException {

        SharedTable.initializeThreadPool(conf.getInt("sharedClient.ThreadPool.size", 50));

        if (LB == null) {
            String error = "Player Load Balancer is not initialized";
            LOG.error(error);
            throw new IllegalStateException(error);
        }


        if (threadPool == null) {
            String error = "Thread Pool not initialized";
            LOG.error(error);
            throw new IllegalStateException(error);
        }

        connections = new ArrayList<HTable>();

        for (int i = 1; i < 4; i++) {
            sharedConfig = new SharedClientConfiguration(conf, i);
            Configuration clusterConfig = sharedConfig
                    .createClusterConfiguration();

            HTable clusterTable = new HTable(clusterConfig, tableName);
            connections.add(clusterTable);
        }
        this.schema = schema;
        readLock = TABLE_LOCKS.readLock(tableName);
        writeLock = TABLE_LOCKS.writeLock(tableName);

    }

    public static void initializeLoadBalancer(ResultPlayerLoadBalancer loadBalancer) {
        LB = loadBalancer;
    }

    public synchronized static void initializeThreadPool(int nthreads) {
        if (threadPool == null) {
            threadPool = Executors.newFixedThreadPool(nthreads);
        }
    }

    private Long getRequestId() {
        return identifierGenerator.getAndAdd(1);
    }

    /**
     * @param put
     * @throws java.io.InterruptedIOException
     * @throws RetriesExhaustedWithDetailsException
     */
    public void put(final Put put) throws IOException {
        if (schema.getEncryptionMode()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable protected Put");
            }
            try {
                new MultiPut(this.sharedConfig, connections, schema, put, threadPool).doOperation();
            } catch (InterruptedException | InvalidNumberOfBits | ExecutionException | InvalidSecretValue ex) {
                LOG.error(ex);
                throw new IllegalStateException(ex);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable standard Put");
            }
            this.connections.get(0).put(put);
        }
    }


    public Result get(Get get) throws IOException {

        if (schema.getEncryptionMode()) {

            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable protected Get");
            }

            try {
                MultiGet mGet = new MultiGet(sharedConfig, connections, schema, get, threadPool);
                mGet.doOperation();
                return mGet.getResult();
            } catch (InterruptedException | IOException | ExecutionException ex) {
                LOG.error(ex);
                throw new IllegalStateException(ex);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable unprotected Get");
            }
            return this.connections.get(0).get(get);
        }
    }

    public ResultScanner getScanner(Scan scan) throws IOException {

        if (schema.getEncryptionMode()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable protected getScanner");
            }
            readLock.lock();
            long requestID = getRequestId();
            int targetPlayer = LB.getResultPlayer();
            MultiScan mScan = new MultiScan(sharedConfig, connections, schema, requestID, targetPlayer, scan, threadPool);
            mScan.startScan();
            readLock.unlock();
            return mScan;
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable unprotected getScanner");
            }
            return this.connections.get(0).getScanner(scan);

        }

    }


    public byte[] getTableName() {
        return this.connections.get(0).getTableName();
    }

    public TableName getName() {
        return this.connections.get(0).getName();
    }

    public Configuration getConfiguration() {
        return this.connections.get(0).getConfiguration();
    }

    public HTableDescriptor getTableDescriptor() throws IOException {
        return this.connections.get(0).getTableDescriptor();
    }

    public boolean exists(Get get) throws IOException {
        return this.connections.get(0).exists(get);
    }

    public Boolean[] exists(List<Get> list) throws IOException {
        return this.connections.get(0).exists(list);
    }

    public void batch(List<? extends Row> list, Object[] os) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Object[] batch(List<? extends Row> list) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public <R> void batchCallback(List<? extends Row> list, Object[] os, Batch.Callback<R> clbck) {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public <R> Object[] batchCallback(List<? extends Row> list, Batch.Callback<R> clbck) {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public Result[] get(List<Get> list) throws IOException {
        if (schema.getEncryptionMode()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable protected bach get");
            }
            Result[] res = new Result[list.size()];
            int offset = 0;
            for (Get g : list) {
                res[offset] = this.get(g);
                offset += 1;
            }

            return res;
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable unprotected bach get");
            }
            return this.connections.get(0).get(list);
        }
    }

    public Result getRowOrBefore(byte[] bytes, byte[] bytes1) {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public ResultScanner getScanner(byte[] bytes) {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public ResultScanner getScanner(byte[] startRow, byte[] stopRow) {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public void put(List<Put> list) throws IOException {
        if (schema.getEncryptionMode()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable protected batch put");
            }
            try {
                new MultiPut(this.sharedConfig, connections, schema, list, threadPool).doOperation();
            } catch (InvalidSecretValue | InterruptedException | InvalidNumberOfBits | ExecutionException ex) {
                LOG.error(ex);
                throw new IllegalStateException(ex);
            }
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable unprotected batch put");
            }
            this.connections.get(0).put(list);
        }

    }

    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
                               byte[] value, Put put) throws IOException {
        if (schema.getEncryptionMode()) {

            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable protected checkAndPut");
            }
            boolean res;
            writeLock.lock();
            try {
                long requestID = getRequestId();
                int targetPlayer = LB.getResultPlayer();
                MultiCheckAndPut op = new MultiCheckAndPut(this.sharedConfig, connections, schema, row, family, qualifier, value, requestID, targetPlayer, put, threadPool);
                op.doOperation();
                res = op.getResult();
            } catch (InvalidSecretValue | InterruptedException | InvalidNumberOfBits | ExecutionException ex) {
                LOG.error(ex);
                throw new IllegalStateException(ex);
            } finally {
                writeLock.unlock();
            }

            return res;
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable unprotected checkAndPut");
            }
            return this.connections.get(0).checkAndPut(row, family, qualifier, value, put);
        }
    }

    public void delete(Delete delete) throws IOException {
        if (schema.getEncryptionMode()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable protected delete");
            }
            writeLock.lock();
            MultiDelete mDel = new MultiDelete(sharedConfig, connections, schema, delete, threadPool);
            deleteOP(mDel);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable unprotected delete");
            }
            this.connections.get(0).delete(delete);
        }

    }

    public void delete(List<Delete> list) throws IOException {
        if (schema.getEncryptionMode()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable protected batch delete");
            }
            writeLock.lock();
            MultiDelete mDel = new MultiDelete(sharedConfig, connections, schema, list, threadPool);
            deleteOP(mDel);
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable unprotected batch delete");
            }
            this.connections.get(0).delete(list);
        }
    }

    private void deleteOP(MultiDelete mDel) throws IOException {
        try {
            mDel.doOperation();
        } catch (InterruptedException | ExecutionException ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        } finally {
            writeLock.unlock();
        }
    }

    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier, byte[] value, Delete delete) {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public void mutateRow(RowMutations rm) {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public Result append(Append append) {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public Result increment(Increment i) {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    /**
     * This operation is currently not supported for secret shared data since
     * the Clinidata use case does not require it. However, if a future use case appears
     * that requires this method, than it can be supported with secretsharing. It is only a matter of
     * generating a share for the required amount to increment and issue the request.
     * <p>
     * For now just assume that the target column is not protected and send the method to the tables.
     * CryptoTable actually does the verification of the protected column.
     */
    public long incrementColumnValue(byte[] bytes, byte[] bytes1,
                                     byte[] bytes2, long l) throws IOException {
        if (schema.getEncryptionMode()) {

            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable protected incrementColumnValue");
            }

            long result = 0;

            for (HTable table : this.connections) {
                result = table.incrementColumnValue(bytes, bytes1, bytes2, l);
            }

            return result;
        } else {
            if (LOG.isDebugEnabled()) {
                LOG.debug("SharedTable unprotected incrementColumnValue");
            }
            return this.connections.get(0).incrementColumnValue(bytes, bytes1, bytes2, l);
        }
    }

    public long incrementColumnValue(byte[] bytes, byte[] bytes1, byte[] bytes2, long l, Durability drblt) {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public long incrementColumnValue(byte[] bytes, byte[] bytes1, byte[] bytes2, long l, boolean bln) {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public boolean isAutoFlush() {
        return this.connections.get(0).isAutoFlush();
    }

    public void setAutoFlush(boolean bln) {
        if (schema.getEncryptionMode()) {

            for (HTable table : this.connections) {
                table.setAutoFlushTo(bln);
            }
        } else {
            this.connections.get(0).setAutoFlushTo(bln);
        }
    }

    public void flushCommits() throws IOException {
        if (schema.getEncryptionMode()) {
            for (HTable table : connections) {
                table.flushCommits();
            }
        } else {
            this.connections.get(0).flushCommits();
        }

    }

    public void close() throws IOException {
        if (schema.getEncryptionMode()) {

            for (HTable table : connections) {
                table.close();
            }
        } else {
            this.connections.get(0).close();
        }
    }

    public <T extends Service, R> Map<byte[], R> coprocessorService(
            Class<T> type, byte[] bytes, byte[] bytes1, Batch.Call<T, R> call) {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public <T extends Service, R> void coprocessorService(Class<T> type,
                                                          byte[] bytes, byte[] bytes1, Batch.Call<T, R> call,
                                                          Batch.Callback<R> clbck) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void setAutoFlush(boolean bln, boolean bln1) {
        if (schema.getEncryptionMode()) {
            for (HTable table : this.connections) {
                table.setAutoFlush(bln, bln1);
            }
        } else {
            this.connections.get(0).setAutoFlush(bln, bln1);
        }
    }

    public void setAutoFlushTo(boolean bln) {
        if (schema.getEncryptionMode()) {

            for (HTable table : this.connections) {
                table.setAutoFlushTo(bln);
            }
        } else {
            this.connections.get(0).setAutoFlushTo(bln);
        }
    }

    public long getWriteBufferSize() {
        return connections.get(0).getWriteBufferSize();
    }

    public void setWriteBufferSize(long l) throws IOException {
        if (schema.getEncryptionMode()) {

            for (HTable table : this.connections) {
                table.setWriteBufferSize(l);
            }
        } else {
            this.connections.get(0).setWriteBufferSize(l);
        }

    }

    public <R extends Message> Map<byte[], R> batchCoprocessorService(
            Descriptors.MethodDescriptor md, Message msg, byte[] bytes,
            byte[] bytes1, R r) {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public <R extends Message> void batchCoprocessorService(
            Descriptors.MethodDescriptor md, Message msg, byte[] bytes,
            byte[] bytes1, R r, Batch.Callback<R> clbck) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean checkAndMutate(byte[] bytes, byte[] bytes1, byte[] bytes2,
                                  CompareFilter.CompareOp co, byte[] bytes3, RowMutations rm) {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    @Override
    public HRegionLocation getRegionLocation(byte[] row){
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public byte[][] getStartKeys() throws IOException {
        return this.connections.get(0).getStartKeys();
    }

    @Override
    public Pair getStartEndKeys() throws IOException {
        return this.connections.get(0).getStartEndKeys();
    }

    @Override
    public List getRegionsInRange(byte[] bytes, byte[] bytes1) throws IOException {
        return this.connections.get(0).getRegionsInRange(bytes, bytes1);
    }

    @Override
    public NavigableMap getRegionLocations() throws IOException {
        return this.connections.get(0).getRegionLocations();
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] bytes) {
        return this.connections.get(0).coprocessorService(bytes);
    }

}


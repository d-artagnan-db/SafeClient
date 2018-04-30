package pt.uminho.haslab.safeclient.shareclient;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
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
    private final String tableName;
    private final List<HTable> connections;
    private SharedClientConfiguration sharedConfig;
    private TableSchema schema;

    public SharedTable(Configuration conf, String tableName, TableSchema schema)
            throws IOException, InvalidNumberOfBits {

        if (LB == null) {
            String error = "Player Load Balancer is not initialized";
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
        this.tableName = tableName;
    }

    public static void initializeLoadBalancer(
            ResultPlayerLoadBalancer loadBalancer) {
        LB = loadBalancer;
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Put operation");
        }
        try {
            Lock writeLock = TABLE_LOCKS.writeLock(tableName);
            writeLock.lock();
            new MultiPut(this.sharedConfig, connections, schema, put).doOperation();
            writeLock.unlock();
        } catch (InterruptedException | InvalidNumberOfBits | InvalidSecretValue ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }
    }


    public Result get(Get get) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Get operation");
        }
        Lock readLock = TABLE_LOCKS.readLock(tableName);
        readLock.lock();
        try {
            MultiGet mGet = new MultiGet(sharedConfig, connections, schema, get);
            mGet.doOperation();
            return mGet.getResult();
        } catch (InterruptedException | IOException ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        } finally {
            readLock.unlock();
        }
    }

    public ResultScanner getScanner(Scan scan) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getScanner");
        }
        Lock readLock = TABLE_LOCKS.readLock(tableName);
        readLock.lock();

        long requestID = getRequestId();
        int targetPlayer = LB.getResultPlayer();
        MultiScan mScan = new MultiScan(sharedConfig, connections, schema, requestID, targetPlayer, scan);
        mScan.startScan();
        readLock.unlock();
        return mScan;

    }


    public byte[] getTableName() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("GetTableName");
        }
        return this.connections.get(0).getTableName();
    }

    public TableName getName() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getName");
        }

        return this.connections.get(0).getName();
    }

    public Configuration getConfiguration() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getConfiguration");
        }
        return this.connections.get(0).getConfiguration();
    }

    public HTableDescriptor getTableDescriptor() throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("getTableDescriptor");
        }
        return this.connections.get(0).getTableDescriptor();
    }

    public boolean exists(Get get) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("exists Get");
        }
        return this.connections.get(0).exists(get);
    }

    public Boolean[] exists(List<Get> list) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("exits list get");
        }
        return this.connections.get(0).exists(list);
    }

    public void batch(List<? extends Row> list, Object[] os)
            throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Object[] batch(List<? extends Row> list) throws IOException,
            InterruptedException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public <R> void batchCallback(List<? extends Row> list, Object[] os,
                                  Batch.Callback<R> clbck) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public <R> Object[] batchCallback(List<? extends Row> list,
                                      Batch.Callback<R> clbck) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public Result[] get(List<Get> list) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public Result getRowOrBefore(byte[] bytes, byte[] bytes1)
            throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public ResultScanner getScanner(byte[] bytes) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public ResultScanner getScanner(byte[] startRow, byte[] stopRow)
            throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public void put(List<Put> list) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Batch put operation");
        }
        try {
            Lock writeLock = TABLE_LOCKS.writeLock(tableName);
            writeLock.lock();
            new MultiPut(this.sharedConfig, connections, schema, list).doOperation();
            writeLock.unlock();
        } catch (InvalidSecretValue | InterruptedException | InvalidNumberOfBits ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }

    }

    public boolean checkAndPut(byte[] row, byte[] family, byte[] qualifier,
                               byte[] value, Put put) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("checkAndPut operation");
        }
        boolean res;
        try {
            Lock writeLock = TABLE_LOCKS.writeLock(tableName);
            writeLock.lock();
            long requestID = getRequestId();
            int targetPlayer = LB.getResultPlayer();
            MultiCheckAndPut op = new MultiCheckAndPut(this.sharedConfig, connections, schema, row, family, qualifier, value, requestID, targetPlayer, put);
            op.doOperation();
            res = op.getResult();
            writeLock.unlock();
        } catch (InvalidSecretValue | InterruptedException | InvalidNumberOfBits ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }
        return res;
    }

    public void delete(Delete delete) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("delete operation");
        }
        Lock writeLock = TABLE_LOCKS.writeLock(tableName);
        writeLock.lock();
        MultiDelete mDel = new MultiDelete(sharedConfig, connections, schema, delete);
        try {
            mDel.doOperation();
        } catch (InterruptedException ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }
        writeLock.unlock();

    }

    public void delete(List<Delete> list) throws IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Batch delete operation");
        }
        Lock writeLock = TABLE_LOCKS.writeLock(tableName);
        writeLock.lock();
        MultiDelete mDel = new MultiDelete(sharedConfig, connections, schema, list);
        try {
            mDel.doOperation();
        } catch (InterruptedException ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }
        writeLock.unlock();
    }

    public boolean checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
                                  byte[] value, Delete delete) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public void mutateRow(RowMutations rm) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public Result append(Append append) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public Result increment(Increment i) throws IOException {
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("incrementColumnValue");
        }
        long result = 0;
        for (HTable table : this.connections) {
            result = table.incrementColumnValue(bytes, bytes1, bytes2, l);
        }

        return result;
    }

    public long incrementColumnValue(byte[] bytes, byte[] bytes1,
                                     byte[] bytes2, long l, Durability drblt) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public long incrementColumnValue(byte[] bytes, byte[] bytes1,
                                     byte[] bytes2, long l, boolean bln) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public boolean isAutoFlush() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("isAutoFlush");
        }
        return this.connections.get(0).isAutoFlush();
    }

    public void setAutoFlush(boolean bln) {
        for (HTable table : this.connections) {
            table.setAutoFlushTo(bln);
        }

    }

    public void flushCommits() throws IOException {
        for (HTable table : connections) {
            table.flushCommits();
        }

    }

    public void close() throws IOException {
        for (HTable table : connections) {
            table.close();
        }
    }

    public CoprocessorRpcChannel coprocessorService(byte[] bytes) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public <T extends Service, R> Map<byte[], R> coprocessorService(
            Class<T> type, byte[] bytes, byte[] bytes1, Batch.Call<T, R> call)
            throws ServiceException, Throwable {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public <T extends Service, R> void coprocessorService(Class<T> type,
                                                          byte[] bytes, byte[] bytes1, Batch.Call<T, R> call,
                                                          Batch.Callback<R> clbck) throws ServiceException, Throwable {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void setAutoFlush(boolean bln, boolean bln1) {
        for (HTable table : this.connections) {
            table.setAutoFlush(bln, bln1);
        }
    }

    public void setAutoFlushTo(boolean bln) {
        for (HTable table : this.connections) {
            table.setAutoFlush(bln);
        }
    }

    public long getWriteBufferSize() {
        return connections.get(0).getWriteBufferSize();
    }

    public void setWriteBufferSize(long l) throws IOException {
        for (HTable table : this.connections) {
            table.setWriteBufferSize(l);
        }

    }

    public <R extends Message> Map<byte[], R> batchCoprocessorService(
            Descriptors.MethodDescriptor md, Message msg, byte[] bytes,
            byte[] bytes1, R r) throws ServiceException, Throwable {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    public <R extends Message> void batchCoprocessorService(
            Descriptors.MethodDescriptor md, Message msg, byte[] bytes,
            byte[] bytes1, R r, Batch.Callback<R> clbck)
            throws ServiceException, Throwable {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean checkAndMutate(byte[] bytes, byte[] bytes1, byte[] bytes2,
                                  CompareFilter.CompareOp co, byte[] bytes3, RowMutations rm)
            throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");

    }

    @Override
    public HRegionLocation getRegionLocation(byte[] row) throws IOException {
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
}

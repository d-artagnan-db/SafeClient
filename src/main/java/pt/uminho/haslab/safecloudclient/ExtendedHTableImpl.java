package pt.uminho.haslab.safecloudclient;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ExtendedHTableImpl implements ExtendedHTable {

    private HTable table;

    public ExtendedHTableImpl(Configuration conf, String tableName) throws IOException {
        table = new HTable(conf, tableName);
    }

    @Override
    public HRegionLocation getRegionLocation(byte[] row) throws IOException {
        return table.getRegionLocation(row);
    }

    @Override
    public byte[] getTableName() {
        return table.getTableName();
    }

    @Override
    public TableName getName() {
        return table.getName();
    }

    @Override
    public Configuration getConfiguration() {
        return table.getConfiguration();
    }

    @Override
    public HTableDescriptor getTableDescriptor() throws IOException {
        return table.getTableDescriptor();
    }

    @Override
    public boolean exists(Get get) throws IOException {
        return table.exists(get);
    }

    @Override
    public Boolean[] exists(List<Get> list) throws IOException {
        return table.exists(list);
    }

    @Override
    public void batch(List<? extends Row> list, Object[] objects) throws IOException, InterruptedException {
        table.batch(list, objects);
    }

    @Override
    public Object[] batch(List<? extends Row> list) throws IOException, InterruptedException {
        return table.batch(list);
    }

    @Override
    public <R> void batchCallback(List<? extends Row> list, Object[] objects, Batch.Callback<R> callback) throws IOException, InterruptedException {
        table.batchCallback(list, objects, callback);
    }

    @Override
    public <R> Object[] batchCallback(List<? extends Row> list, Batch.Callback<R> callback) throws IOException, InterruptedException {
        return table.batchCallback(list, callback);
    }

    @Override
    public Result get(Get get) throws IOException {
        return table.get(get);
    }

    @Override
    public Result[] get(List<Get> list) throws IOException {
        return table.get(list);
    }

    @Override
    public Result getRowOrBefore(byte[] bytes, byte[] bytes1) throws IOException {
        return table.getRowOrBefore(bytes, bytes1);
    }

    @Override
    public ResultScanner getScanner(Scan scan) throws IOException {
        return table.getScanner(scan);
    }

    @Override
    public ResultScanner getScanner(byte[] bytes) throws IOException {
        return table.getScanner(bytes);
    }

    @Override
    public ResultScanner getScanner(byte[] bytes, byte[] bytes1) throws IOException {
        return table.getScanner(bytes, bytes1);
    }

    @Override
    public void put(Put put) throws IOException {
        table.put(put);
    }

    @Override
    public void put(List<Put> list) throws IOException {
        table.put(list);
    }

    @Override
    public boolean checkAndPut(byte[] bytes, byte[] bytes1, byte[] bytes2, byte[] bytes3, Put put) throws IOException {
        return table.checkAndPut(bytes, bytes1, bytes2, bytes3, put);
    }

    @Override
    public void delete(Delete delete) throws IOException {
        table.delete(delete);
    }

    @Override
    public void delete(List<Delete> list) throws IOException {
        table.delete(list);
    }

    @Override
    public boolean checkAndDelete(byte[] bytes, byte[] bytes1, byte[] bytes2, byte[] bytes3, Delete delete) throws IOException {
        return table.checkAndDelete(bytes, bytes1, bytes2, bytes3, delete);
    }

    @Override
    public void mutateRow(RowMutations rowMutations) throws IOException {
        table.mutateRow(rowMutations);
    }

    @Override
    public Result append(Append append) throws IOException {
        return table.append(append);
    }

    @Override
    public Result increment(Increment increment) throws IOException {
        return table.increment(increment);
    }

    @Override
    public long incrementColumnValue(byte[] bytes, byte[] bytes1, byte[] bytes2, long l) throws IOException {
        return table.incrementColumnValue(bytes, bytes1, bytes2, l);
    }

    @Override
    public long incrementColumnValue(byte[] bytes, byte[] bytes1, byte[] bytes2, long l, Durability durability) throws IOException {
        return  table.incrementColumnValue(bytes, bytes1, bytes2, l, durability);
    }

    @Override
    public long incrementColumnValue(byte[] bytes, byte[] bytes1, byte[] bytes2, long l, boolean b) throws IOException {
        return table.incrementColumnValue(bytes, bytes1, bytes2, l, b);
    }

    @Override
    public boolean isAutoFlush() {
        return table.isAutoFlush();
    }

    @Override
    public void flushCommits() throws IOException {
        table.flushCommits();
    }

    @Override
    public void close() throws IOException {
        table.close();
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] bytes) {
        return table.coprocessorService(bytes);
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(Class<T> aClass, byte[] bytes, byte[] bytes1, Batch.Call<T, R> call) throws ServiceException, Throwable {
        return table.coprocessorService(aClass, bytes, bytes1, call);
    }

    @Override
    public <T extends Service, R> void coprocessorService(Class<T> aClass, byte[] bytes, byte[] bytes1, Batch.Call<T, R> call, Batch.Callback<R> callback) throws ServiceException, Throwable {
        table.coprocessorService(aClass, bytes, bytes1, call, callback);
    }

    @Override
    public void setAutoFlush(boolean b) {
        table.setAutoFlush(b);
    }

    @Override
    public void setAutoFlush(boolean b, boolean b1) {
        table.setAutoFlush(b, b1);
    }

    @Override
    public void setAutoFlushTo(boolean b) {
        table.setAutoFlush(b);
    }

    @Override
    public long getWriteBufferSize() {
        return table.getWriteBufferSize();
    }

    @Override
    public void setWriteBufferSize(long l) throws IOException {
        table.setWriteBufferSize(l);
    }

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes1, R r) throws ServiceException, Throwable {
        return table.batchCoprocessorService(methodDescriptor, message, bytes, bytes1, r);
    }

    @Override
    public <R extends Message> void batchCoprocessorService(Descriptors.MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes1, R r, Batch.Callback<R> callback) throws ServiceException, Throwable {
        table.batchCoprocessorService(methodDescriptor, message, bytes, bytes1, r, callback);
    }

    @Override
    public boolean checkAndMutate(byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp, byte[] bytes3, RowMutations rowMutations) throws IOException {
        return table.checkAndMutate(bytes, bytes1, bytes2, compareOp, bytes3, rowMutations);
    }
}

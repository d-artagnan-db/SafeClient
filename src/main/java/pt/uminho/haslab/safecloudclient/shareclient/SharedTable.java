package pt.uminho.haslab.safecloudclient.shareclient;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import pt.uminho.haslab.safecloudclient.shareclient.conccurentops.MultiGet;
import pt.uminho.haslab.safecloudclient.shareclient.conccurentops.MultiPut;
import pt.uminho.haslab.safecloudclient.shareclient.conccurentops.MultiScan;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Dealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindDealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;

/**
 * This current implementation works only for a single client and assuming each
 * cluster has the same number of keys.
 */
public class SharedTable implements HTableInterface {

	static final Log LOG = LogFactory.getLog(SharedTable.class.getName());

	private final List<HTable> connections;
	private SharedClientConfiguration sharedConfig;
	private final Dealer dealer;
	/*
	 * Currently we are assuming a single client that is inserting the
	 * values.This does not work if either have multiple clients or the client
	 * inserts and than reconects to insert again.
	 */
	private static final AtomicLong lastMaxKey = new AtomicLong();

	public SharedTable(Configuration conf, String tableName)
			throws IOException, InvalidNumberOfBits {
		connections = new ArrayList<HTable>();

		for (int i = 1; i < 4; i++) {
			sharedConfig = new SharedClientConfiguration(conf, i);
			Configuration clusterConfig = sharedConfig
					.createClusterConfiguration();

			HTable clusterTable = new HTable(clusterConfig, tableName);
			connections.add(clusterTable);
		}
		dealer = new SharemindDealer(sharedConfig.getNBits());

	}

	private long getMaximumKey() throws IOException {
		return lastMaxKey.getAndAdd(1);
	}

	/**
	 * 
	 * TODO:The put has to check if there is a key with the same value
	 * 
	 * @param put
	 * @throws java.io.InterruptedIOException
	 * @throws org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException
	 */
	@Override
	public void put(final Put put) throws InterruptedIOException,
			RetriesExhaustedWithDetailsException, IOException {
		try {

			List<byte[]> secrets = getSecretKey(put.getRow());

			long requestID = getMaximumKey();
			byte[] maxKey = ("" + requestID).getBytes();

			MultiPut mput = new MultiPut(this.sharedConfig, connections,
					secrets, requestID, 1, put, maxKey);
			mput.doOperation();
		} catch (InvalidSecretValue ex) {
			LOG.error(ex);
            throw new IllegalStateException(ex);
		} catch (InterruptedException ex) {
			LOG.error(ex);
            throw new IllegalStateException(ex);

		}
	}

	public void scanAndPrint(int id) throws IOException {
		Scan scan = new Scan();

		ResultScanner rs = connections.get(id).getScanner(scan);
		byte[] columnFam = sharedConfig.getShareKeyColumnFamily().getBytes();
		byte[] columnQual = sharedConfig.getShareKeyColumnQualifier()
				.getBytes();

		for (Result r = rs.next(); r != null; r = rs.next()) {

			BigInteger val = new BigInteger(r.getValue(columnFam, columnQual));
			System.out.println(new BigInteger(r.getRow()) + " -- " + val);
		}
		rs.close();

	}

	@Override
	public Result get(Get get) throws IOException {
		try {
			List<byte[]> secrets = getSecretKey(get.getRow());

			long requestID = getMaximumKey();
			MultiGet mGet = new MultiGet(sharedConfig, connections, secrets,
					requestID, 1);
			mGet.doOperation();
			return mGet.getResult();

		} catch (InvalidSecretValue ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} catch (InterruptedException ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		}
	}

	private List<byte[]> getSecretKey(byte[] row) throws InvalidSecretValue {

		BigInteger originalKey = new BigInteger(row);
		SharemindSharedSecret secrets = (SharemindSharedSecret) dealer
				.share(originalKey);
		List<byte[]> byteSecrets = new ArrayList<byte[]>();

		byteSecrets.add(secrets.getU1().toByteArray());
		byteSecrets.add(secrets.getU2().toByteArray());
		byteSecrets.add(secrets.getU3().toByteArray());

		return byteSecrets;
	}

	@Override
	public byte[] getTableName() {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public TableName getName() {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public Configuration getConfiguration() {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public HTableDescriptor getTableDescriptor() throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public boolean exists(Get get) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public Boolean[] exists(List<Get> list) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public void batch(List<? extends Row> list, Object[] os)
			throws IOException, InterruptedException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public Object[] batch(List<? extends Row> list) throws IOException,
			InterruptedException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public <R> void batchCallback(List<? extends Row> list, Object[] os,
			Batch.Callback<R> clbck) throws IOException, InterruptedException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public <R> Object[] batchCallback(List<? extends Row> list,
			Batch.Callback<R> clbck) throws IOException, InterruptedException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public Result[] get(List<Get> list) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public Result getRowOrBefore(byte[] bytes, byte[] bytes1)
			throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public ResultScanner getScanner(Scan scan) throws IOException {

		try {
			long requestID = getMaximumKey();
			if (scan.getStartRow().length != 0 && scan.getStopRow().length != 0) {
				List<byte[]> startRowSecrets = getSecretKey(scan.getStartRow());
				List<byte[]> stopRowSecrets = getSecretKey(scan.getStopRow());
				MultiScan ms = new MultiScan(sharedConfig, connections,
						requestID, 1, startRowSecrets, stopRowSecrets);
				ms.startScan();
				return ms;

			} else if (scan.getStartRow().length != 0
					&& scan.getStopRow().length == 0) {
				List<byte[]> startRowSecrets = getSecretKey(scan.getStartRow());
				List<byte[]> stopRowSecrets = new ArrayList<byte[]>();
				MultiScan ms = new MultiScan(sharedConfig, connections,
						requestID, 1, startRowSecrets, stopRowSecrets);
				ms.startScan();
				return ms;
			} else if (scan.getStartRow().length == 0
					&& scan.getStopRow().length != 0) {
				List<byte[]> startRowSecrets = new ArrayList<byte[]>();
				List<byte[]> stopRowSecrets = getSecretKey(scan.getStopRow());
				MultiScan ms = new MultiScan(sharedConfig, connections,
						requestID, 1, startRowSecrets, stopRowSecrets);
				ms.startScan();
                return ms;
			} else if (scan.getStartRow().length == 0
					&& scan.getStopRow().length == 0) {
				List<byte[]> startRowSecrets = new ArrayList<byte[]>();
				List<byte[]> stopRowSecrets = new ArrayList<byte[]>();
				MultiScan ms = new MultiScan(sharedConfig, connections,
						requestID, 1, startRowSecrets, stopRowSecrets);
				ms.startScan();
				return ms;
			}
		} catch (InvalidSecretValue ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		}
		return null;
	}

	@Override
	public ResultScanner getScanner(byte[] bytes) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public ResultScanner getScanner(byte[] startRow, byte[] stopRow)
			throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public void put(List<Put> list) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public boolean checkAndPut(byte[] bytes, byte[] bytes1, byte[] bytes2,
			byte[] bytes3, Put put) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public void delete(Delete delete) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public void delete(List<Delete> list) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public boolean checkAndDelete(byte[] bytes, byte[] bytes1, byte[] bytes2,
			byte[] bytes3, Delete delete) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public void mutateRow(RowMutations rm) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public Result append(Append append) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public Result increment(Increment i) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public long incrementColumnValue(byte[] bytes, byte[] bytes1,
			byte[] bytes2, long l) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public long incrementColumnValue(byte[] bytes, byte[] bytes1,
			byte[] bytes2, long l, Durability drblt) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public long incrementColumnValue(byte[] bytes, byte[] bytes1,
			byte[] bytes2, long l, boolean bln) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public boolean isAutoFlush() {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public void flushCommits() throws IOException {
		for (HTable table : connections) {
			table.flushCommits();
		}

	}

	@Override
	public void close() throws IOException {
		for (HTable table : connections) {
			table.close();
		}

	}

	@Override
	public CoprocessorRpcChannel coprocessorService(byte[] bytes) {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public <T extends Service, R> Map<byte[], R> coprocessorService(
			Class<T> type, byte[] bytes, byte[] bytes1, Batch.Call<T, R> call)
			throws ServiceException, Throwable {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public <T extends Service, R> void coprocessorService(Class<T> type,
			byte[] bytes, byte[] bytes1, Batch.Call<T, R> call,
			Batch.Callback<R> clbck) throws ServiceException, Throwable {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public void setAutoFlush(boolean bln) {
		for (HTable table : this.connections) {
			table.setAutoFlushTo(bln);
		}

	}

	@Override
	public void setAutoFlush(boolean bln, boolean bln1) {
		for (HTable table : this.connections) {
			table.setAutoFlush(bln, bln1);
		}
	}

	@Override
	public void setAutoFlushTo(boolean bln) {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public long getWriteBufferSize() {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public void setWriteBufferSize(long l) throws IOException {
		for (HTable table : this.connections) {
			table.setWriteBufferSize(l);
		}

	}

	@Override
	public <R extends Message> Map<byte[], R> batchCoprocessorService(
			Descriptors.MethodDescriptor md, Message msg, byte[] bytes,
			byte[] bytes1, R r) throws ServiceException, Throwable {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public <R extends Message> void batchCoprocessorService(
			Descriptors.MethodDescriptor md, Message msg, byte[] bytes,
			byte[] bytes1, R r, Batch.Callback<R> clbck)
			throws ServiceException, Throwable {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public boolean checkAndMutate(byte[] bytes, byte[] bytes1, byte[] bytes2,
			CompareFilter.CompareOp co, byte[] bytes3, RowMutations rm)
			throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

}

package pt.uminho.haslab.safecloudclient.shareclient;

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
import pt.uminho.haslab.safecloudclient.ExtendedHTable;
import pt.uminho.haslab.safecloudclient.shareclient.conccurentops.MultiGet;
import pt.uminho.haslab.safecloudclient.shareclient.conccurentops.MultiPut;
import pt.uminho.haslab.safecloudclient.shareclient.conccurentops.MultiScan;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Dealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindDealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * Current implementation assumes that tables identifiers are not protected with secret sharing and only columns
 * can be protected. Thus, any necessary processing over those columns is only triggered by Filters.
 * This version supports distributed clusters with multiple region servers.
 */
public class SharedTable implements ExtendedHTable {

	static final Log LOG = LogFactory.getLog(SharedTable.class.getName());

	private static final AtomicLong lastMaxKey = new AtomicLong();
	private static final AtomicLong identifierGenerator = new AtomicLong();
	private static ResultPlayerLoadBalancer LB = new ResultPlayerLoadBalancerImpl();

        
	public static void initializeLoadBalancer(
			ResultPlayerLoadBalancer loadBalancer) {
		LB = loadBalancer;
	}

	private final List<HTable> connections;
	private final Dealer dealer;
	private SharedClientConfiguration sharedConfig;
	private final String tableName;

	private TableSchema schema;

	private long getNextRowIdentifier() throws IOException {
		return lastMaxKey.getAndAdd(1);
	}

	private byte[] convKey(Long requestID) throws IOException {
		return ("" + requestID).getBytes();
	}

	private Long getRequestId() {
		return identifierGenerator.getAndAdd(1);
	}

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

		dealer = new SharemindDealer(sharedConfig.getNBits());
		this.tableName = tableName;
		this.schema = schema;
	}

	/**
	 *
	 * @param put
	 * @throws java.io.InterruptedIOException
	 * @throws RetriesExhaustedWithDetailsException
	 */
	public void put(final Put put) throws IOException {
		try{
            new MultiPut(this.sharedConfig, connections, put, schema).doOperation();
        } catch (InvalidSecretValue | InterruptedException | InvalidNumberOfBits ex) {
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

	public Result get(Get get) throws IOException {

	}

	private MultiGet handleGet(Get get, boolean isCached, byte[] cachedID) {
		try {
			List<byte[]> secrets = getSecretKey(get.getRow());

			long requestID = getRequestId();
			int targetPlayer = LB.getResultPlayer();
			LOG.debug("Going t start MultiGet");
			MultiGet mGet = new MultiGet(sharedConfig, connections, secrets,
					requestID, targetPlayer, isCached, cachedID);
			mGet.doOperation();

			Result res = mGet.getResult();
			LOG.debug(Thread.currentThread().getId()
					+ " Result found is emptpy? " + res.isEmpty());
			if (!res.isEmpty()) {
				LOG.debug(Thread.currentThread().getId()
						+ " Storing in cache value " + mGet.getUniqueRowId());
				CACHE.put(tableName, get.getRow(), mGet.getUniqueRowId());
			}

			return mGet;

		} catch (InvalidSecretValue ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} catch (InterruptedException ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} catch (IOException ex) {
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

	public byte[] getTableName() {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	public TableName getName() {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	public Configuration getConfiguration() {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	public HTableDescriptor getTableDescriptor() throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	public boolean exists(Get get) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	public Boolean[] exists(List<Get> list) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

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

	public ResultScanner getScanner(Scan scan) throws IOException {
		Lock readLock = TABLE_LOCKS.readLock(tableName);
		try {
			readLock.lock();
			long requestID = getRequestId();
			int targetPlayer = LB.getResultPlayer();

			if (scan.getStartRow().length != 0 && scan.getStopRow().length != 0) {
				List<byte[]> startRowSecrets = getSecretKey(scan.getStartRow());
				List<byte[]> stopRowSecrets = getSecretKey(scan.getStopRow());
				MultiScan ms = new MultiScan(sharedConfig, connections,
						requestID, targetPlayer, startRowSecrets,
						stopRowSecrets);
				ms.startScan();
				return ms;

			} else if (scan.getStartRow().length != 0
					&& scan.getStopRow().length == 0) {
				List<byte[]> startRowSecrets = getSecretKey(scan.getStartRow());
				List<byte[]> stopRowSecrets = new ArrayList<byte[]>();
				MultiScan ms = new MultiScan(sharedConfig, connections,
						requestID, targetPlayer, startRowSecrets,
						stopRowSecrets);
				ms.startScan();
				return ms;
			} else if (scan.getStartRow().length == 0
					&& scan.getStopRow().length != 0) {
				List<byte[]> startRowSecrets = new ArrayList<byte[]>();
				List<byte[]> stopRowSecrets = getSecretKey(scan.getStopRow());
				MultiScan ms = new MultiScan(sharedConfig, connections,
						requestID, targetPlayer, startRowSecrets,
						stopRowSecrets);
				ms.startScan();
				return ms;
			} else if (scan.getStartRow().length == 0
					&& scan.getStopRow().length == 0) {
				List<byte[]> startRowSecrets = new ArrayList<byte[]>();
				List<byte[]> stopRowSecrets = new ArrayList<byte[]>();
				MultiScan ms = new MultiScan(sharedConfig, connections,
						requestID, targetPlayer, startRowSecrets,
						stopRowSecrets);
				ms.startScan();
				return ms;
			}
		} catch (InvalidSecretValue ex) {
			LOG.error(ex);
			throw new IllegalStateException(ex);
		} finally {
			readLock.unlock();
		}
		return null;
	}

	public ResultScanner getScanner(byte[] bytes) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	public ResultScanner getScanner(byte[] startRow, byte[] stopRow)
			throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	public void put(List<Put> list) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	public boolean checkAndPut(byte[] bytes, byte[] bytes1, byte[] bytes2,
			byte[] bytes3, Put put) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	public void delete(Delete delete) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	public void delete(List<Delete> list) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	public boolean checkAndDelete(byte[] bytes, byte[] bytes1, byte[] bytes2,
			byte[] bytes3, Delete delete) throws IOException {
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

	public long incrementColumnValue(byte[] bytes, byte[] bytes1,
			byte[] bytes2, long l) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

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
		throw new UnsupportedOperationException("Not supported yet.");

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
		throw new UnsupportedOperationException("Not supported yet.");

	}

	public long getWriteBufferSize() {
		throw new UnsupportedOperationException("Not supported yet.");

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
		return null;
	}
}

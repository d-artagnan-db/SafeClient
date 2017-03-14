package pt.uminho.haslab.safecloudclient.shareclient;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
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

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 * Current implementation only works with a single regionServer or standalone.
 */
public class SharedTable implements HTableInterface {

	static final Log LOG = LogFactory.getLog(SharedTable.class.getName());

	private static final AtomicLong lastMaxKey = new AtomicLong();
	private static final AtomicLong identifierGenerator = new AtomicLong();
	private static ResultPlayerLoadBalancer LB = new ResultPlayerLoadBalancerImpl();
	private static ClientCache CACHE;
	private static final TableLock TABLE_LOCKS = new TableLockImpl();
       
        
	public static void initializeLoadBalancer(
			ResultPlayerLoadBalancer loadBalancer) {
		LB = loadBalancer;
	}

	public static void initalizeCache(ClientCache cache) {
		CACHE = cache;
	}

	private final List<HTable> connections;
	private final Dealer dealer;
	private SharedClientConfiguration sharedConfig;
	private final String tableName;

	private long getNextRowIdentifier() throws IOException {
		return lastMaxKey.getAndAdd(1);
	}

	private byte[] convKey(Long requestID) throws IOException {
		return ("" + requestID).getBytes();
	}

	private Long getRequestId() {
		return identifierGenerator.getAndAdd(1);
	}

	public SharedTable(Configuration conf, String tableName)
			throws IOException, InvalidNumberOfBits {

		if (LB == null || CACHE == null) {
			String error = "SharedTable static variables not initialized";
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
	}

	/**
	 * TODO:The put has to check if there is a key with the same value
	 * 
	 * @param put
	 * @throws java.io.InterruptedIOException
	 * @throws RetriesExhaustedWithDetailsException
	 */
	public void put(final Put put) throws IOException {
		LOG.debug("Going to do put operation");
		byte[] row = put.getRow();
		Lock writeLock = TABLE_LOCKS.writeLock(tableName);
		writeLock.lock();
		LOG.debug(Thread.currentThread().getId() + " Put after Lock");
		try {

			LOG.debug("Checking if cache contains value for row " + row
					+ " in table " + tableName);
			boolean rowInCache = CACHE.containsKey(tableName, row);
			LOG.debug("Row in cache is " + rowInCache);

			byte[] putRow;
			if (rowInCache) {
				Long longID = CACHE.get(tableName, row);
				LOG.debug("Found unique id on CACHE " + longID);
				putRow = convKey(longID);
			} else {
				Get get = new Get(row);

				MultiGet mGet = handleGet(get, false, null);
				Result res = mGet.getResult();
				Long rowID;

				if (res.isEmpty()) {
					LOG.debug("No value stored on the database");
					rowID = getNextRowIdentifier();
					putRow = convKey(rowID);
					LOG.debug("Storing unique id " + rowID
							+ " on cache with key " + row + " for table"
							+ tableName);
					/**
					 * The row as not been inserted on HBase yet, so insert in
					 * the cache before storing in the db.
					 */
					CACHE.put(tableName, row, rowID);
				} else {
					LOG.debug("An identifier already exists " + res.getRow()
							+ " with unique key " + mGet.getUniqueRowId());
					/**
					 * Row is already inserted in the cache on the handleGet
					 * function if the result is not empty.
					 */
					putRow = convKey(mGet.getUniqueRowId());
				}
			}
			LOG.debug("Going to handlePut");
			handlePut(putRow, put);
		} finally {
			LOG.debug("Exiting put function");
			writeLock.unlock();
		}

	}

	private void handlePut(byte[] rowID, final Put put) throws IOException {
		try {

			List<byte[]> secrets = getSecretKey(put.getRow());
			Long requestID = getRequestId();
			MultiPut mput = new MultiPut(this.sharedConfig, connections,
					secrets, requestID, 1, put, rowID);
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

	public Result get(Get get) throws IOException {
		LOG.debug("Going to do get operation");
		byte[] row = get.getRow();
		Lock readLock = TABLE_LOCKS.readLock(tableName);
		readLock.lock();
		Result res;

		try {
			boolean rowInCache = CACHE.containsKey(tableName, row);

			if (rowInCache) {
				Long rowID = CACHE.get(tableName, row);
				LOG.debug(Thread.currentThread().getId()
						+ " Retrieved unique id on cache " + rowID);
				res = handleGet(get, true, convKey(rowID)).getResult();
			} else {
				LOG.debug(Thread.currentThread().getId()
						+ " Key not stored on cache");
				res = handleGet(get, false, null).getResult();
			}
		} finally {
			readLock.unlock();
		}
		return res;
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

}

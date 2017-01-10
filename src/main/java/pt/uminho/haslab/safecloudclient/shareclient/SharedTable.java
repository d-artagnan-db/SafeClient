package pt.uminho.haslab.safecloudclient.shareclient;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
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
import org.apache.hadoop.hbase.client.Query;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
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
	private static AtomicLong lastMaxKey = new AtomicLong();

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

		/*
		 * byte[][] endKeys = connections.get(0).getEndKeys();
		 * System.out.println("Number of inserted keys "+endKeys.length);
		 * BigInteger max = BigInteger.ZERO; for (byte[] endKey : endKeys) {
		 * System.out.println("End key length "+endKey.length);
		 * System.out.println("End key "+Arrays.toString(endKey)); BigInteger
		 * endKeyValue = BigInteger.ZERO;
		 * 
		 * if(endKey.length > 0){ endKeyValue = new BigInteger(endKey); } if
		 * (endKeyValue.compareTo(max) == 1) { max = endKeyValue; }
		 * 
		 * }
		 */
		// int current = lastMaxKey.get();
		// lastMaxKey += 1;
		return lastMaxKey.getAndAdd(1);
	}

	private Put createSecretRegionPut(Put put, byte[] identifier,
			byte[] virtualKey) throws IOException {
		Put secretPut = new Put(identifier);
		CellScanner cs = put.cellScanner();

		while (cs.advance()) {
			Cell cell = cs.current();

			secretPut.add(CellUtil.cloneFamily(cell),
					CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
		}

		secretPut.add(sharedConfig.getShareKeyColumnFamily().getBytes(),
				sharedConfig.getShareKeyColumnQualifier().getBytes(),
				virtualKey);
		return secretPut;

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

			byte[] maxKey = ("" + getMaximumKey()).getBytes();

			final Put secretPutOne = createSecretRegionPut(put, maxKey,
					secrets.get(0));
			final Put secretPutTwo = createSecretRegionPut(put, maxKey,
					secrets.get(1));
			final Put secretPutThree = createSecretRegionPut(put, maxKey,
					secrets.get(2));
			connections.get(0).put(secretPutOne);
			connections.get(1).put(secretPutTwo);
			connections.get(2).put(secretPutThree);

		} catch (InvalidSecretValue ex) {
			Logger.getLogger(SharedTable.class.getName()).log(Level.SEVERE,
					null, ex);
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
			// System.out.println("Secrets");
			long requestID = getMaximumKey();
			System.out.println("RequestID is " + requestID);
			MultiGet mGet = new MultiGet(get, secrets, requestID, 1);
			return mGet.doOperation();

		} catch (InvalidSecretValue ex) {
			// System.out.println(ex);
			LOG.debug(ex);
			throw new IllegalStateException(ex);
		} catch (InterruptedException ex) {
			// System.out.println(ex);
			LOG.debug(ex);
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

	private abstract class MultiOP {

		protected final Query oldQuery;
		protected final List<byte[]> secrets;

		protected final long requestID;
		protected final int targetPlayer;

		public MultiOP(Query oldQuery, List<byte[]> secrets, long requestID,
				int targetPlayer) {
			this.oldQuery = oldQuery;
			this.secrets = secrets;
			this.requestID = requestID;
			this.targetPlayer = targetPlayer;
		}

		protected abstract Thread queryThread(HTable table, byte[] row);

		protected abstract Result decodeResults(List<Thread> threads)
				throws IOException;

		public Result doOperation() throws InterruptedException, IOException {
			List<Thread> calls = new ArrayList<Thread>();

			for (int i = 0; i < secrets.size(); i++) {
				HTable table = connections.get(i);
				byte[] row = secrets.get(i);
				calls.add(queryThread(table, row));
			}

			// System.out.println("Going to issue get request");
			for (Thread t : calls) {
				t.start();

			}
			// System.out.println("Going to wait for calls to be issued");
			for (Thread t : calls) {
				t.join();
			}
			// System.out.println("Get calls terminated");

			return decodeResults(calls);
		}
	}

	private class MultiGet extends MultiOP {

		public MultiGet(Query oldQuery, List<byte[]> secrets, long requestID,
				int targetPlayer) {
			super(oldQuery, secrets, requestID, targetPlayer);
		}

		@Override
		protected Thread queryThread(HTable table, byte[] row) {
			return new GetThread(table, oldQuery, row, requestID, targetPlayer);
		}

		@Override
		protected Result decodeResults(List<Thread> threads) throws IOException {
			byte[] secretColFam = sharedConfig.getShareKeyColumnFamily()
					.getBytes();
			byte[] secretColQual = sharedConfig.getShareKeyColumnQualifier()
					.getBytes();

			Result resOne = ((QueryThread) threads.get(0)).getResult();
			Result resTwo = ((QueryThread) threads.get(1)).getResult();
			Result resThree = ((QueryThread) threads.get(2)).getResult();

			byte[] rowSecretOne = resOne.getValue(secretColFam, secretColQual);
			byte[] rowSecretTwo = resTwo.getValue(secretColFam, secretColQual);
			byte[] rowSecretThree = resThree.getValue(secretColFam,
					secretColQual);

			BigInteger firstSecret = new BigInteger(rowSecretOne);
			BigInteger secondSecret = new BigInteger(rowSecretTwo);
			BigInteger thirdSecret = new BigInteger(rowSecretThree);

			SharemindSharedSecret secret = new SharemindSharedSecret(
					sharedConfig.getNBits(), firstSecret, secondSecret,
					thirdSecret);
			byte[] resRow = secret.unshare().toByteArray();

			CellScanner firstScanner = resOne.cellScanner();
			CellScanner secondScanner = resTwo.cellScanner();
			CellScanner thirdScanner = resThree.cellScanner();
			List<Cell> cells = new ArrayList<Cell>();

			while (firstScanner.advance() && secondScanner.advance()
					&& thirdScanner.advance()) {
				Cell firstCell = firstScanner.current();
				Cell secondCell = secondScanner.current();
				Cell thirdCell = thirdScanner.current();
				List<byte[]> values = new ArrayList<byte[]>();
				byte[] fValue = CellUtil.cloneValue(firstCell);
				byte[] sValue = CellUtil.cloneValue(secondCell);
				byte[] tValue = CellUtil.cloneValue(thirdCell);
				values.add(fValue);
				values.add(sValue);
				values.add(tValue);
				byte[] value = oneTimeDecode(values);

				byte[] cf = CellUtil.cloneFamily(firstCell);
				byte[] cq = CellUtil.cloneQualifier(secondCell);
				byte type = firstCell.getTypeByte();
				long timestamp = firstCell.getTimestamp();

				Cell decCell = CellUtil.createCell(resRow, cf, cq, timestamp,
						type, value);
				cells.add(decCell);
			}

			return Result.create(cells);

		}

	}

	public List<byte[]> oneTimePadEncode(byte[] value) {
		List<byte[]> encValues = new ArrayList<byte[]>();

		SecureRandom random = new SecureRandom();
		byte firstRandom[] = new byte[value.length];
		byte secondRandom[] = new byte[value.length];

		random.nextBytes(firstRandom);
		random.nextBytes(secondRandom);
		encValues.add(firstRandom);
		encValues.add(secondRandom);

		BigInteger bfRandom = new BigInteger(firstRandom);
		BigInteger bsRandom = new BigInteger(secondRandom);
		BigInteger bvRandom = new BigInteger(value);

		byte encValue[] = bfRandom.xor(bsRandom).xor(bvRandom).toByteArray();

		encValues.add(encValue);
		return encValues;
	}

	public byte[] oneTimeDecode(List<byte[]> values) {
		BigInteger firstSecret = new BigInteger(values.get(0));
		BigInteger secondSecret = new BigInteger(values.get(1));
		BigInteger thirdSecret = new BigInteger(values.get(2));
		return firstSecret.xor(secondSecret).xor(thirdSecret).toByteArray();
	}

	private static abstract class QueryThread extends Thread {

		protected final Query oldQuery;

		protected final byte[] secretRow;

		protected final HTable table;

		protected Result res;

		protected final long requestID;

		protected final int targetPlayer;

		public QueryThread(HTable table, Query oldQuery, byte[] secretRow,
				long requestID, int targetPlayer) {
			this.oldQuery = oldQuery;
			this.secretRow = secretRow;
			this.table = table;
			this.requestID = requestID;
			this.targetPlayer = targetPlayer;
		}

		public Result getResult() {
			return res;
		}

		protected abstract Result query() throws IOException;

		@Override
		public void run() {
			try {
				res = query();
			} catch (IOException ex) {
				LOG.debug(ex);
			}

		}

	}

	private class GetThread extends QueryThread {

		public GetThread(HTable table, Query oldQuery, byte[] secretRow,
				long requestID, int targetPlayer) {
			super(table, oldQuery, secretRow, requestID, targetPlayer);
		}

		@Override
		protected Result query() throws IOException {

			Get get = new Get(secretRow);
			get.setAttribute("requestID", ("" + requestID).getBytes());
			get.setAttribute("targetPlayer", ("" + targetPlayer).getBytes());

			return table.get(get);
		}

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
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public ResultScanner getScanner(byte[] bytes) throws IOException {
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public ResultScanner getScanner(byte[] bytes, byte[] bytes1)
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
		throw new UnsupportedOperationException("Not supported yet.");

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
		throw new UnsupportedOperationException("Not supported yet.");

	}

	@Override
	public void setAutoFlush(boolean bln, boolean bln1) {
		throw new UnsupportedOperationException("Not supported yet.");

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
		throw new UnsupportedOperationException("Not supported yet.");

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

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
import pt.uminho.haslab.safeclient.ExtendedHTable;
import pt.uminho.haslab.safeclient.shareclient.conccurentops.MultiGet;
import pt.uminho.haslab.safeclient.shareclient.conccurentops.MultiPut;
import pt.uminho.haslab.safeclient.shareclient.conccurentops.MultiScan;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;

import java.io.IOException;
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

	private static final AtomicLong identifierGenerator = new AtomicLong();
	private static ResultPlayerLoadBalancer LB = new ResultPlayerLoadBalancerImpl();

    private static final TableLock TABLE_LOCKS = new TableLockImpl();
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
	 *
	 * @param put
	 * @throws java.io.InterruptedIOException
	 * @throws RetriesExhaustedWithDetailsException
	 */
	public void put(final Put put) throws IOException {
		try{
            Lock writeLock = TABLE_LOCKS.writeLock(tableName);
            writeLock.lock();
            new MultiPut(this.sharedConfig, connections, schema, put).doOperation();
            writeLock.unlock();
        } catch (InvalidSecretValue | InterruptedException | InvalidNumberOfBits ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
		}
    }


	public Result get(Get get) throws IOException {
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
        for (HTable table : this.connections) {
            table.coprocessorService(type, bytes, bytes1, call, clbck);
        }
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

        for (HTable table : this.connections) {
            table.batchCoprocessorService(md, msg, bytes, bytes1, r, clbck);
        }
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

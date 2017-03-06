package pt.uminho.haslab.safecloudclient.shareclient.conccurentops;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import pt.uminho.haslab.safecloudclient.shareclient.SharedClientConfiguration;
import static pt.uminho.haslab.safecloudclient.shareclient.conccurentops.OneTimePad.oneTimeDecode;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;

public abstract class MultiOP {

	static final org.apache.commons.logging.Log LOG = LogFactory
			.getLog(MultiOP.class.getName());

	protected final long requestID;
	protected final int targetPlayer;
	protected final List<HTable> connections;
	protected final SharedClientConfiguration config;

	protected Long uniqueRowId;

	public MultiOP(SharedClientConfiguration config, List<HTable> connections,
			long requestID, int targetPlayer) {
		this.requestID = requestID;
		this.targetPlayer = targetPlayer;
		this.connections = connections;
		this.config = config;
	}

	protected abstract Thread queryThread(SharedClientConfiguration config,
			HTable table, int index);

	protected abstract void threadsJoined(List<Thread> threads)
			throws IOException;

	protected Result decodeResult(List<Result> results) throws IOException {
		byte[] secretColFam = config.getShareKeyColumnFamily().getBytes();
		byte[] secretColQual = config.getShareKeyColumnQualifier().getBytes();

		Result resOne = results.get(0);
		Result resTwo = results.get(1);
		Result resThree = results.get(2);
		LOG.debug("Row of match result is " + new String(resOne.getRow()));
		this.uniqueRowId = Long.valueOf(new String(resOne.getRow()));

		byte[] rowSecretOne = resOne.getValue(secretColFam, secretColQual);
		byte[] rowSecretTwo = resTwo.getValue(secretColFam, secretColQual);
		byte[] rowSecretThree = resThree.getValue(secretColFam, secretColQual);

		BigInteger firstSecret = new BigInteger(rowSecretOne);
		BigInteger secondSecret = new BigInteger(rowSecretTwo);
		BigInteger thirdSecret = new BigInteger(rowSecretThree);

		SharemindSharedSecret secret = new SharemindSharedSecret(
				config.getNBits(), firstSecret, secondSecret, thirdSecret);
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
			byte[] cf = CellUtil.cloneFamily(firstCell);
			byte[] cq = CellUtil.cloneQualifier(secondCell);

			// Filter columns holding secrets
			if (!(Arrays
					.equals(cf, config.getShareKeyColumnFamily().getBytes()) && Arrays
					.equals(cq, config.getShareKeyColumnQualifier().getBytes()))) {
				List<byte[]> values = new ArrayList<byte[]>();
				byte[] fValue = CellUtil.cloneValue(firstCell);
				byte[] sValue = CellUtil.cloneValue(secondCell);
				byte[] tValue = CellUtil.cloneValue(thirdCell);
				values.add(fValue);
				values.add(sValue);
				values.add(tValue);
				byte[] value = oneTimeDecode(values);

				byte type = firstCell.getTypeByte();
				long timestamp = firstCell.getTimestamp();

				Cell decCell = CellUtil.createCell(resRow, cf, cq, timestamp,
						type, value);
				cells.add(decCell);
			}
		}

		return Result.create(cells);
	}

	public void doOperation() throws InterruptedException, IOException {
		List<Thread> calls = new ArrayList<Thread>();
		int index = 0;
		for (HTable table : connections) {
			calls.add(queryThread(config, table, index));
			index += 1;
		}

		LOG.debug("Going to do request");
		for (Thread t : calls) {
			t.start();

		}
		LOG.debug("Going to wait for calls to be issued");
		for (Thread t : calls) {
			t.join();
		}

		LOG.debug("Operation calls terminated");
		threadsJoined(calls);
	}

	public void startScan() {
		List<Thread> calls = new ArrayList<Thread>();
		int index = 0;
		for (HTable table : connections) {
			calls.add(queryThread(config, table, index));
			index += 1;
		}

		LOG.debug("Going to issue get request");

		for (Thread t : calls) {
			t.start();
		}

	}

	public Long getUniqueRowId() {
		return this.uniqueRowId;
	}
}
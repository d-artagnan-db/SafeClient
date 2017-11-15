package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import pt.uminho.haslab.safeclient.shareclient.SharedClientConfiguration;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public abstract class MultiOP {

	static final org.apache.commons.logging.Log LOG = LogFactory
			.getLog(MultiOP.class.getName());

	protected final List<HTable> connections;
	protected final SharedClientConfiguration config;
	protected final TableSchema schema;
	protected Long uniqueRowId;

	public MultiOP(SharedClientConfiguration config, List<HTable> connections, TableSchema schema) {
		this.connections = connections;
		this.config = config;
		this.schema = schema;
	}

	protected abstract Thread queryThread(SharedClientConfiguration config,
                                          HTable table, int index) throws IOException;

	protected abstract void threadsJoined(List<Thread> threads)
			throws IOException;

	protected Result decodeResult(List<Result> results) throws IOException {


		Result resOne = results.get(0);
		Result resTwo = results.get(1);
		Result resThree = results.get(2);

		LOG.debug("Row of match result is " + new String(resOne.getRow()));
		this.uniqueRowId = Long.valueOf(new String(resOne.getRow()));


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
            byte[] row = CellUtil.cloneRow(firstCell);

            String family = new String(cf, Charset.forName("UTF-8"));
            String qualifier = new String(cq, Charset.forName("UTF-8"));
            byte[] decValue = CellUtil.cloneValue(firstCell);
            byte type = firstCell.getTypeByte();
            long timestamp = firstCell.getTimestamp();

            // Revert secrets back to original value
            if (schema.getCryptoTypeFromQualifier(family, qualifier) == DatabaseSchema.CryptoType.SMPC) {
                // FormatSize defines the size of the ring in which the shares generated

				int formatSize = schema.getFormatSizeFromQualifier(family, qualifier);
                byte[] fSecret = CellUtil.cloneValue(firstCell);
                byte[] sSecret = CellUtil.cloneValue(secondCell);
                byte[] tSecret = CellUtil.cloneValue(thirdCell);

                BigInteger firstSecret = new BigInteger(fSecret);
                BigInteger secondSecret = new BigInteger(sSecret);
                BigInteger thirdSecret = new BigInteger(tSecret);

                SharemindSharedSecret secret = new SharemindSharedSecret(formatSize, firstSecret, secondSecret, thirdSecret);
                decValue = secret.unshare().toByteArray();
               // LOG.debug("Decoding secrets " + firstSecret + ":" + secondSecret + ":" + thirdSecret + " into val " + new BigInteger(decValue));
            }
            Cell decCell = CellUtil.createCell(row, cf, cq, timestamp,
                    type, decValue);
            cells.add(decCell);
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

		for (Thread t: calls) {
			t.start();
		}

		for (Thread t: calls) {
			t.join();
		}

		threadsJoined(calls);
	}

    public void startScan() throws IOException {
        List<Thread> calls = new ArrayList<Thread>();
        int index = 0;
		for (HTable table : connections) {
			calls.add(queryThread(config, table, index));
			index += 1;
		}

		LOG.debug("Going to start scan request");

		for (Thread t : calls) {
			t.start();
		}

	}

	public Long getUniqueRowId() {
		return this.uniqueRowId;
	}
}
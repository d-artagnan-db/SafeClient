package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import pt.uminho.haslab.safeclient.shareclient.SharedClientConfiguration;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.smpc.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smpc.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smpc.interfaces.Dealer;
import pt.uminho.haslab.smpc.sharemindImp.BigInteger.SharemindDealer;
import pt.uminho.haslab.smpc.sharemindImp.BigInteger.SharemindSharedSecret;
import pt.uminho.haslab.smpc.sharemindImp.Integer.IntSharemindDealer;
import pt.uminho.haslab.smpc.sharemindImp.Long.LongSharemindDealer;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public abstract class MultiOP {

	static final org.apache.commons.logging.Log LOG = LogFactory
			.getLog(MultiOP.class.getName());

    private static final AtomicLong cellVersions = new AtomicLong(0);

	public static IntSharemindDealer iDealer = new IntSharemindDealer();
	public static LongSharemindDealer lDealer = new LongSharemindDealer();


	protected final List<HTable> connections;
	protected final SharedClientConfiguration config;
	protected final TableSchema schema;
    protected byte[] uniqueRowId;

    protected final ExecutorService threadPool;


	public MultiOP(SharedClientConfiguration config, List<HTable> connections, TableSchema schema, ExecutorService threadPool) {
		this.connections = connections;
		this.config = config;
		this.schema = schema;
		this.threadPool = threadPool;
	}

	protected abstract Runnable queryThread(SharedClientConfiguration config,
                                          HTable table, int index) throws IOException;

	protected abstract void threadsJoined(List<Runnable> threads)
			throws IOException;


    protected List<Put> generateMPCPut(Put originalPut) throws IOException, InvalidNumberOfBits, InvalidSecretValue {

        byte[] row = originalPut.getRow();
        List<Put> putResults = new ArrayList<Put>();
        long putVersion = cellVersions.addAndGet(1);
        for (int i = 0; i < 3; i++) {
            putResults.add(new Put(row));
        }

        CellScanner scanner = originalPut.cellScanner();
        while (scanner.advance()) {
            Cell cell = scanner.current();
            byte[] value = CellUtil.cloneValue(cell);
            byte[] bCF = CellUtil.cloneFamily(cell);
            byte[] bCQ = CellUtil.cloneQualifier(cell);
            String family = new String(bCF, Charset.forName("UTF-8"));
            String qualifier = new String(bCQ, Charset.forName("UTF-8"));
            List<byte[]> values = new ArrayList<byte[]>();
            DatabaseSchema.CryptoType type = schema.getCryptoTypeFromQualifier(family, qualifier);

            switch (type) {
                case SMPC:

                    int formatSize = schema.getFormatSizeFromQualifier(family, qualifier);
                    Dealer dealer = new SharemindDealer(formatSize);

                    BigInteger bigVal = new BigInteger(value);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Sharing SMPC value " + bigVal + " with formatsize "+ formatSize +" for column " + family +":"+qualifier);
                    }
                    SharemindSharedSecret secret = (SharemindSharedSecret) dealer.share(bigVal);

                    values.add(secret.getU1().toByteArray());
                    values.add(secret.getU2().toByteArray());
                    values.add(secret.getU3().toByteArray());
                    break;
                case ISMPC:

                    int ptxValue = ByteBuffer.wrap(value).getInt();
                    int[] shares = iDealer.share(ptxValue);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Sharing ISMPC value " + ptxValue + " in values " + Arrays.toString(shares));
                    }

                    for (int share : shares) {
                        values.add(Bytes.toBytes(share));
                    }

                    break;
                case LSMPC:
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Sharing LSMPC");
                    }
                    long lValue = ByteBuffer.wrap(value).getLong();
                    long[] lshares = lDealer.share(lValue);

                    for (long lshare : lshares) {
                        values.add(Bytes.toBytes(lshare));
                    }
                    break;
                case XOR:
                    if(LOG.isDebugEnabled()){
                        LOG.debug("Encoding with XOR");
                    }
                    byte[][] xvalues = OneTimePad.oneTimePadEncode(value);
                    values.addAll(Arrays.asList(xvalues));
                    break;
                default:
                    values.add(value);
                    values.add(value);
                    values.add(value);

            }

            for (int i = 0; i < putResults.size(); i++) {
                putResults.get(i).add(bCF, bCQ, putVersion, values.get(i));
            }

        }
        return putResults;
    }

    protected Result decodeResult(List<Result> results) throws IOException {


		Result resOne = results.get(0);
		Result resTwo = results.get(1);
		Result resThree = results.get(2);
        this.uniqueRowId = resOne.getRow();

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

            DatabaseSchema.CryptoType ctype = schema.getCryptoTypeFromQualifier(family, qualifier);
            switch (ctype) {

                case SMPC:
                    // FormatSize defines the size of the ring in which the shares generated
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Decode SMPC");
                    }
                    int formatSize = schema.getFormatSizeFromQualifier(family, qualifier);
                    byte[] fSecret = CellUtil.cloneValue(firstCell);
                    byte[] sSecret = CellUtil.cloneValue(secondCell);
                    byte[] tSecret = CellUtil.cloneValue(thirdCell);

                    BigInteger firstSecret = new BigInteger(fSecret);
                    BigInteger secondSecret = new BigInteger(sSecret);
                    BigInteger thirdSecret = new BigInteger(tSecret);

                    SharemindSharedSecret secret = new SharemindSharedSecret(formatSize, firstSecret, secondSecret, thirdSecret);
                    decValue = secret.unshare().toByteArray();
                    break;
                case ISMPC:


                    int[] shares = new int[3];
                    shares[0] = ByteBuffer.wrap(CellUtil.cloneValue(firstCell)).getInt();
                    shares[1] = ByteBuffer.wrap(CellUtil.cloneValue(secondCell)).getInt();
                    shares[2] = ByteBuffer.wrap(CellUtil.cloneValue(thirdCell)).getInt();

                    int plxValue = iDealer.unshare(shares);
                    decValue = Bytes.toBytes(plxValue);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Decode ISMPC value " + plxValue);
                    }
                    break;
                case LSMPC:
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Decode LSMPC");
                    }
                    long[] lshares = new long[3];
                    lshares[0] = ByteBuffer.wrap(CellUtil.cloneValue(firstCell)).getLong();
                    lshares[1] = ByteBuffer.wrap(CellUtil.cloneValue(secondCell)).getLong();
                    lshares[2] = ByteBuffer.wrap(CellUtil.cloneValue(thirdCell)).getLong();

                    long lValue = lDealer.unshare(lshares);
                    decValue = Bytes.toBytes(lValue);
                    break;
                case XOR:
                    byte[][] xors = new byte[3][];
                    xors[0] = CellUtil.cloneValue(firstCell);
                    xors[1] = CellUtil.cloneValue(secondCell);
                    xors[2] = CellUtil.cloneValue(thirdCell);

                    decValue = OneTimePad.oneTimeDecode(xors);
                    break;

            }
            Cell decCell = CellUtil.createCell(row, cf, cq, timestamp,
                    type, decValue);
            cells.add(decCell);
        }

		return Result.create(cells);
	}


	public void doOperation() throws InterruptedException, IOException, ExecutionException {
		List<Runnable> calls = new ArrayList<Runnable>();
		List<Future> futures = new ArrayList<Future>();
		int index = 0;
		for (HTable table : connections) {
			calls.add(queryThread(config, table, index));
			index += 1;
		}

		for (Runnable t: calls) {
			futures.add(threadPool.submit(t));
		}

		for (Future t: futures) {
			t.get();
		}

		threadsJoined(calls);
	}

    public void startScan() throws IOException {
        List<Runnable> calls = new ArrayList<Runnable>();
        List<Future> futures = new ArrayList<Future>();

        int index = 0;
		for (HTable table : connections) {
			calls.add(queryThread(config, table, index));
			index += 1;
		}

		if(LOG.isDebugEnabled()){
		    LOG.debug("Going to start scan request");
		}

		for (Runnable t : calls) {
			futures.add(threadPool.submit(t));
		}

		joinThreads(futures);

	}

    protected abstract void  joinThreads(List<Future> threads)
            throws IOException;

    public byte[] getUniqueRowId() {
        return this.uniqueRowId;
	}
}
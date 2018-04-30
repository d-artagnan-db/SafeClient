package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import pt.uminho.haslab.protocommunication.Smpc;
import pt.uminho.haslab.safeclient.shareclient.SharedClientConfiguration;
import pt.uminho.haslab.saferegions.OperationAttributesIdentifiers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;

public class ResultScannerThread extends QueryThread implements ResultScanner {

    private final LinkedBlockingDeque<Result> results;
    private ResultScanner resultScanner;
    private Scan scan;

    private boolean hasProtectedScan;

    public ResultScannerThread(SharedClientConfiguration config, HTable table, Scan scan, boolean hasProtectedScan)
            throws IOException {
        super(config, table);
        results = new LinkedBlockingDeque<Result>();
        this.hasProtectedScan = hasProtectedScan;

        if (!hasProtectedScan || !conf.hasConcurrentScanEndpoint()) {
            resultScanner = table.getScanner(scan);
        } else if (conf.hasConcurrentScanEndpoint()) {
            this.scan = scan;
        }

    }

    public Result next() throws IOException {
        try {
            return results.take();
        } catch (InterruptedException ex) {
            LOG.error(ex);
            throw new IllegalStateException(ex);
        }
    }

    public Result[] next(int i) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void close() {
        if (!conf.hasConcurrentScanEndpoint()) {
            resultScanner.close();
        }
    }

    public Iterator<Result> iterator() {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    private void handleCoprocessorService(Map<byte[], Smpc.Results> coprocessorResults) {
        for (Smpc.Results res : coprocessorResults.values()) {
            List<Smpc.Row> rows = res.getRowsList();
            for (Smpc.Row r : rows) {
                List<Smpc.Cell> cells = r.getCellsList();
                List<Cell> resCells = new ArrayList<Cell>();
                for (Smpc.Cell cell : cells) {
                    byte[] row = cell.getRow().toByteArray();
                    byte[] family = cell.getColumnFamily().toByteArray();
                    byte[] qualifier = cell.getColumnQualifier().toByteArray();
                    long timestamp = cell.getTimestamp();
                    byte[] type = cell.getType().toByteArray();
                    byte[] value = cell.getValue().toByteArray();
                    Cell resCell = CellUtil.createCell(row, family, qualifier, timestamp, type[0], value);
                    resCells.add(resCell);
                }
                results.add(Result.create(resCells));
            }
        }
    }

    @Override
    protected void query() throws IOException {

        if (LOG.isDebugEnabled()) {
            LOG.debug("HasProtectedScan " + hasProtectedScan + " hasConcurrentScanEndpoint " + conf.hasConcurrentScanEndpoint());
        }
        if (!hasProtectedScan || !conf.hasConcurrentScanEndpoint()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Scan to SmpcCoprocessor");
            }
            for (Result result = resultScanner.next(); result != null; result = resultScanner
                    .next()) {
                results.add(result);
            }
        } else if (conf.hasConcurrentScanEndpoint()) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Scan to ConcurrentScanEndpoint");
                }
                byte[] requestID = scan.getAttribute(OperationAttributesIdentifiers.RequestIdentifier);
                int targetPlayer = Integer.parseInt(new String(scan.getAttribute(OperationAttributesIdentifiers.TargetPlayer)));
                EndpointCallback callback = new EndpointCallback(scan, requestID, targetPlayer);
                Map<byte[], Smpc.Results> coprocessorResults = table.coprocessorService(Smpc.ConcurrentScanService.class, null, null, callback);
                handleCoprocessorService(coprocessorResults);
            } catch (Throwable throwable) {
                LOG.error(throwable);
                throw new IllegalStateException(throwable);
            }
        } else {
            throw new IllegalStateException("Scan case not handled");
        }
        results.add(Result.EMPTY_RESULT);
    }

    public class EndpointCallback implements Batch.Call<Smpc.ConcurrentScanService, Smpc.Results> {

        private final Scan scan;
        private byte[] requestID;
        private int targetPlayer;

        public EndpointCallback(Scan scan, byte[] requestID, int targetPlayer) {
            this.scan = scan;
            this.requestID = requestID;
            this.targetPlayer = targetPlayer;
        }

        @Override
        public Smpc.Results call(Smpc.ConcurrentScanService concurrentScanService) throws IOException {
            ServerRpcController controller = new ServerRpcController();
            BlockingRpcCallback<Smpc.Results> rpcCallback =
                    new BlockingRpcCallback<Smpc.Results>();

            Smpc.ScanMessage message = Smpc.ScanMessage.newBuilder()
                    .setStartRow(ByteString.copyFrom(scan.getStartRow()))
                    .setStopRow(ByteString.copyFrom(scan.getStopRow()))
                    .setFilter(ByteString.copyFrom(scan.getFilter().toByteArray()))
                    .setTargetPlayer(targetPlayer)
                    .setFilterType(scan.getFilter().getClass().getName())
                    .setRequestID(ByteString.copyFrom(requestID)).build();

            concurrentScanService.scan(controller, message, rpcCallback);
            if (controller.failedOnException()) {
                throw controller.getFailedOn();
            }
            return rpcCallback.get();
        }
    }

}

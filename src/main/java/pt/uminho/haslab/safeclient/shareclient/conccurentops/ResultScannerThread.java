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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class ResultScannerThread extends QueryThread implements ResultScanner {

    private final BlockingQueue<Result> results;
    private ResultScanner resultScanner;
    private Scan scan;

    private boolean hasProtectedScan;
    private boolean isRunning;

    public ResultScannerThread(SharedClientConfiguration config, HTable table, Scan scan, boolean hasProtectedScan)
            throws IOException {
        super(config, table);
        int queueSize = config.getScanQueueSize();
        results = new ArrayBlockingQueue<Result>(config.getScanQueueSize());
        this.hasProtectedScan = hasProtectedScan;

        if (!hasProtectedScan || !conf.hasConcurrentScanEndpoint()) {
            if(LOG.isDebugEnabled()){
                LOG.debug("Queue size is " + queueSize);
                LOG.debug("Starting ResultScanner for scan " + scan);
            }
            resultScanner = table.getScanner(scan);
        } else if (conf.hasConcurrentScanEndpoint()) {
            this.scan = scan;
        }
        isRunning = true;

    }

    public Result next(){
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Going to close ResultScannerThread " + (!hasProtectedScan || !conf.hasConcurrentScanEndpoint()));
        }

        isRunning = false;
        results.clear();
    }

    public Iterator<Result> iterator() {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    private void handleCoprocessorService(Map<byte[], Smpc.Results> coprocessorResults) throws InterruptedException {
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
                Result rnew = Result.create(resCells);
                results.put(rnew);
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
            int count = 0;
            Result result;
            for ( result = resultScanner.next(); result != null && isRunning; result = resultScanner
                    .next()) {
                count++;
                try {
                    results.put(result);
                } catch (InterruptedException e) {
                    LOG.error(e);
                    throw new IllegalStateException(e);

                }
            }
            if(!isRunning){
                resultScanner.close();
                results.clear();
            }
            if(LOG.isDebugEnabled()) {
                LOG.debug("Going to exit ResultScannerThread smpcoprocessor branch " + count);
            }
        } else if (conf.hasConcurrentScanEndpoint()) {
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Scan to ConcurrentScanEndpoint");
                }
                byte[] requestID = scan.getAttribute(OperationAttributesIdentifiers.RequestIdentifier);
                int targetPlayer = Integer.parseInt(new String(scan.getAttribute(OperationAttributesIdentifiers.TargetPlayer)));
                EndpointCallback callback = new EndpointCallback(scan, requestID, targetPlayer);
                Map<byte[], Smpc.Results> coprocessorResults = table.coprocessorService(Smpc.ConcurrentScanService.class, scan.getStartRow(), scan.getStopRow(), callback);
                handleCoprocessorService(coprocessorResults);
            } catch (Throwable throwable) {
                LOG.error(throwable);
                throw new IllegalStateException(throwable);
            }

        } else {
            throw new IllegalStateException("Scan case not handled");
        }

        try {
            results.put(Result.EMPTY_RESULT);
        } catch (InterruptedException e) {
            LOG.error(e);
            throw new IllegalStateException(e);
        }

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

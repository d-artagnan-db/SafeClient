package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.safeclient.shareclient.SharedClientConfiguration;
import pt.uminho.haslab.safemapper.DatabaseSchema;
import pt.uminho.haslab.safemapper.TableSchema;
import pt.uminho.haslab.saferegions.OperationAttributesIdentifiers;
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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class MultiScan extends MultiOP implements ResultScanner {

    static final Log LOG = LogFactory.getLog(MultiScan.class.getName());
    private final List<Runnable> scans;
    private TableSchema schema;
    private long requestID;
    private long targetPlayer;
    private Scan scan;
    private boolean hasProtectedScan;
    private List<Scan> protectedScans;
    private List<Future> futures;


    public MultiScan(SharedClientConfiguration config,
                     List<HTable> connections, TableSchema schema, long requestID, int targetPlayer, Scan scan, ExecutorService threadPool) {
        super(config, connections, schema, threadPool);
        this.scan = scan;
        this.requestID = requestID;
        this.targetPlayer = targetPlayer;
        this.schema = schema;
        scans = new ArrayList<Runnable>();
        this.protectedScans = new ArrayList<Scan>();
        generateSecureScans();
    }

    private void generateSecureScans() {

        if(LOG.isDebugEnabled()){
            LOG.debug("Scan has filter " + scan.hasFilter());
            LOG.debug("Scan is " + scan);
        }
        if (scan.hasFilter()) {
            Filter originalFilter = scan.getFilter();
            List<Filter> parsedFilters = handleFilterWithProtectedColumns(originalFilter);
            if (hasProtectedScan) {
                List<Scan> scans = plainScans();
                assert parsedFilters != null;
                for (int i = 0; i < scans.size(); i++) {
                    Scan scan = scans.get(i);
                    scan.setFilter(parsedFilters.get(i));
                    scan.setAttribute(OperationAttributesIdentifiers.RequestIdentifier, ("" + requestID).getBytes());
                    scan.setAttribute(OperationAttributesIdentifiers.TargetPlayer, ("" + targetPlayer).getBytes());
                    scan.setAttribute(OperationAttributesIdentifiers.ScanType.ProtectedColumnScan.name(), "true".getBytes());
                }
                protectedScans.addAll(scans);
            }
        }

    }

    private List<Scan> plainScans() {
        List<Scan> scans = new ArrayList<Scan>();

        Scan scanC1 = new Scan(scan.getStartRow(), scan.getStopRow());
        Scan scanC2 = new Scan(scan.getStartRow(), scan.getStopRow());
        Scan scanC3 = new Scan(scan.getStartRow(), scan.getStopRow());

        scans.add(scanC1);
        scans.add(scanC2);
        scans.add(scanC3);
        return scans;
    }

    private List<Filter> handleFilterWithProtectedColumns(Filter original) {
       if(LOG.isDebugEnabled()){
         LOG.debug("Original filter " + original);
       }
        if (original instanceof SingleColumnValueFilter) {
            return handleSingleColumnValueFilter((SingleColumnValueFilter) original);
        } else if (original instanceof FilterList) {
            return handleFilterList((FilterList) original);
        } else if (original instanceof WhileMatchFilter) {
            return handleWhileMatchFilter((WhileMatchFilter) original);
        }else if(original instanceof RowFilter){
            return handleRowFilter((RowFilter) original);
        }else {
            throw new IllegalStateException("Filter not supported " + original.getClass().getName());
        }
    }

    private List<Filter> handleWhileMatchFilter(WhileMatchFilter original) {
        Filter f = original.getFilter();
        List<Filter> handledFilter = handleFilterWithProtectedColumns(f);

        List<Filter> resFilters = new ArrayList<Filter>();

        assert handledFilter != null;
        for (Filter filt : handledFilter) {
            resFilters.add(new WhileMatchFilter(filt));
        }

        return resFilters;
    }

    private List<Filter> handleRowFilter(RowFilter filter){
        List<Filter> result = new ArrayList<>();
        result.add(filter);
        result.add(filter);
        result.add(filter);
        return result;
    }


    private List<Filter> handleFilterList(FilterList filter) {
        List<Filter> list = filter.getFilters();
        List<List<Filter>> resultInnerFilters = new ArrayList<List<Filter>>();

        assert list != null;
        for (Filter f : list) {
            resultInnerFilters.add(handleFilterWithProtectedColumns(f));
        }

        List<Filter> results = new ArrayList<Filter>();

        for (int i = 0; i < 3; i++) {
            FilterList fRes = new FilterList(filter.getOperator());

            for (List<Filter> handledFilter : resultInnerFilters) {
                fRes.addFilter(handledFilter.get(i));
            }
            results.add(fRes);
        }
        return results;
    }

    private List<Filter> handleSingleColumnValueFilter(SingleColumnValueFilter filter) {

        byte[] family = filter.getFamily();
        byte[] qualifier = filter.getQualifier();
        byte[] value = filter.getComparator().getValue();

        String sFamily = new String(family, Charset.forName("UTF-8"));
        String sQualifier = new String(qualifier, Charset.forName("UTF-8"));

        CompareFilter.CompareOp operator = filter.getOperator();
        List<Filter> fList = new ArrayList<Filter>();

        DatabaseSchema.CryptoType type = schema.getCryptoTypeFromQualifier(sFamily, sQualifier);

        if(LOG.isDebugEnabled()){
            LOG.debug("CType is  " + type);
        }
        switch (type) {

            case SMPC:

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Generate Protected Filter for column " + sFamily + ":" + sQualifier + " with type " + type);
                }

                hasProtectedScan = true;
                int formatSize = schema.getFormatSizeFromQualifier(sFamily, sQualifier);

                try {
                    Dealer dealer = new SharemindDealer(formatSize);
                    byte[] sQualifierMod = sQualifier.getBytes();
                    BigInteger bigVal = new BigInteger(value);
                    SharemindSharedSecret secret = (SharemindSharedSecret) dealer.share(bigVal);
                    fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, secret.getU1().toByteArray()));
                    fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, secret.getU2().toByteArray()));
                    fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, secret.getU3().toByteArray()));
                } catch (InvalidNumberOfBits | InvalidSecretValue ex) {
                    LOG.error(ex);
                    throw new IllegalStateException(ex);
                }
                break;
            case ISMPC:

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Generate Protected Filter for column " + sFamily + ":" + sQualifier + " with type " + type);
                }

                hasProtectedScan = true;
                IntSharemindDealer dealer = new IntSharemindDealer();
                try {
                    int[] secrets = dealer.share(ByteBuffer.wrap(value).getInt());
                    byte[] sQualifierMod = sQualifier.getBytes();

                    for (int secret : secrets) {
                        ByteBuffer buffer = ByteBuffer.allocate(4);
                        buffer.putInt(secret);
                        buffer.flip();
                        fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, buffer.array()));
                        buffer.clear();
                    }
                } catch (InvalidSecretValue ex) {
                    LOG.error(ex);
                    throw new IllegalStateException(ex);
                }
                break;
            case LSMPC:
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Generate Protected Filter for column " + sFamily + ":" + sQualifier + " with type " + type);
                }

                hasProtectedScan = true;
                LongSharemindDealer lDealer = new LongSharemindDealer();
                try {
                    long[] secrets = lDealer.share(ByteBuffer.wrap(value).getLong());
                    byte[] sQualifierMod = sQualifier.getBytes();

                    for (long secret : secrets) {
                        ByteBuffer buffer = ByteBuffer.allocate(8);
                        buffer.putLong(secret);
                        buffer.flip();
                        fList.add(new SingleColumnValueFilter(family, sQualifierMod, operator, buffer.array()));
                        buffer.clear();
                    }
                } catch (InvalidSecretValue ex) {
                    LOG.error(ex);
                    throw new IllegalStateException(ex);
                }
                break;
            default:
                if(LOG.isDebugEnabled()){
                    LOG.debug("Default case");
                }
                fList.add(filter);
                fList.add(filter);
                fList.add(filter);

        }

        return fList;
    }

    @Override
    protected Runnable queryThread(SharedClientConfiguration config,
                                 HTable table, int index) throws IOException {

        if(LOG.isDebugEnabled()){
            LOG.debug("HasProtected scan on table " + table.getTableDescriptor().getNameAsString() + "? " + hasProtectedScan);
        }

        ResultScannerThread t = null;
        if (hasProtectedScan) {
            t = new ResultScannerThread(config, table, protectedScans.get(index), true);
        } else {
            t = new ResultScannerThread(config, table, scan, false);
        }
        this.scans.add(t);
        return t;
    }

    @Override
    protected void threadsJoined(List<Runnable> threads) throws IOException {
    }

    @Override
    protected void joinThreads(List<Future> threads) throws IOException {
        this.futures = threads;
    }

    public Result next() throws IOException {
        if(LOG.isDebugEnabled()){
            LOG.debug("Requesting next value");
        }
        List<Result> results = new ArrayList<Result>();
        for (Runnable t : scans) {
            Result rst = ((ResultScannerThread) t).next();
            results.add(rst);
        }

        if (results.get(0).isEmpty()) {
            return null;
        }
        return decodeResult(results);
    }

    public Result[] next(int i) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void close() {
        for (Future t : futures) {
            try {
                t.get();
            } catch (InterruptedException | ExecutionException e) {
                LOG.debug(e);
                throw new IllegalStateException(e);
            }
        }
        for(Runnable t: scans){
            ((ResultScannerThread) t).close();
        }

    }

    public Iterator<Result> iterator() {
        if(LOG.isDebugEnabled()){
            LOG.debug("Iterating over records");
        }
        List<Result> resultIterator = new ArrayList<Result>();
        try {

            for(Future t: futures){
                t.get();
            }
            boolean stop = false;
            while(!stop){

                List<Result> results = new ArrayList<Result>();
                for (Runnable t : scans) {
                    Result rst = ((ResultScannerThread) t).next();
                        results.add(rst);
                }
                if(!results.get(0).isEmpty()){
                    resultIterator.add(decodeResult(results));
                }else{
                    stop = true;
                }
            }
        }
        catch (InterruptedException | IOException | ExecutionException e) {
                LOG.error(e);
                throw new IllegalStateException(e);
        }


        return resultIterator.iterator();
    }

}

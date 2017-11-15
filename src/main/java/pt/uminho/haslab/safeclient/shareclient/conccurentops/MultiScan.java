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
import pt.uminho.haslab.smhbase.exceptions.InvalidNumberOfBits;
import pt.uminho.haslab.smhbase.exceptions.InvalidSecretValue;
import pt.uminho.haslab.smhbase.interfaces.Dealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindDealer;
import pt.uminho.haslab.smhbase.sharemindImp.SharemindSharedSecret;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MultiScan extends MultiOP implements ResultScanner {

    static final Log LOG = LogFactory.getLog(MultiScan.class.getName());
    private final List<Thread> scans;
    private TableSchema schema;
    private long requestID;
    private long targetPlayer;
    private Scan scan;
    private boolean hasProtectedScan;
    private List<Scan> protectedScans;


    public MultiScan(SharedClientConfiguration config,
                     List<HTable> connections, TableSchema schema, long requestID, int targetPlayer, Scan scan) {
        super(config, connections, schema);
        this.scan = scan;
        this.requestID = requestID;
        this.targetPlayer = targetPlayer;
        this.schema = schema;
        scans = new ArrayList<Thread>();
        this.protectedScans = new ArrayList<Scan>();
        generateSecureScans();
    }

    private void generateSecureScans() {


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

        if (original instanceof SingleColumnValueFilter) {
            return handleSingleColumnValueFilter((SingleColumnValueFilter) original);
        } else if (original instanceof FilterList) {
            return handleFilterList((FilterList) original);
        } else if (original instanceof WhileMatchFilter) {
            return handleWhileMatchFilter((WhileMatchFilter) original);
        }
        return null;
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
        CompareFilter.CompareOp operator = filter.getOperator();
        List<Filter> fList = new ArrayList<Filter>();
        LOG.debug("Comapring column " + family + ":" + qualifier);
        LOG.debug(DatabaseSchema.isProtectedColumn(schema, family, qualifier));
        if (DatabaseSchema.isProtectedColumn(schema, family, qualifier)) {
            String sFamily = new String(family);
            String sQualifier = new String(qualifier);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Generated Protected Filter for column " + sFamily + ":" + sQualifier);
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
        } else {
            fList.add(filter);
            fList.add(filter);
            fList.add(filter);
        }

        return fList;
    }

    @Override
    protected Thread queryThread(SharedClientConfiguration config,
                                 HTable table, int index) throws IOException {

        if(LOG.isDebugEnabled()){
            LOG.debug("HasProtected scan on table " + table.getTableDescriptor().getNameAsString() + "? " + hasProtectedScan);
        }

        ResultScannerThread t = null;
        if (hasProtectedScan) {
            t = new ResultScannerThread(config, table, protectedScans.get(index));
        } else {
            t = new ResultScannerThread(config, table, scan);
        }
        this.scans.add(t);
        return t;
    }

    @Override
    protected void threadsJoined(List<Thread> threads) throws IOException {
    }

    public Result next() throws IOException {
        List<Result> results = new ArrayList<Result>();
        for (Thread t : scans) {
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
        for (Thread t : scans) {
            ((ResultScannerThread) t).close();
        }
    }

    public Iterator<Result> iterator() {
        throw new UnsupportedOperationException("Not supported yet."); // Templates.
    }

}

package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import pt.uminho.haslab.safeclient.shareclient.SharedClientConfiguration;
import pt.uminho.haslab.safemapper.TableSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class MultiGet extends MultiOP {

    private Result result;
    private Get originalGet;

    public MultiGet(SharedClientConfiguration config, List<HTable> connections, TableSchema schema, Get get, ExecutorService threadPool) {
        super(config, connections, schema, threadPool);
        result = Result.EMPTY_RESULT;
        this.originalGet = get;
        LOG.debug("Input row is " + new String(get.getRow()));
    }

    @Override
    protected Runnable queryThread(SharedClientConfiguration config,
                                   HTable table, int index) {
        return new GetThread(config, table, originalGet);
    }

    public Result getResult() {
        return result;
    }

    @Override
    protected void threadsJoined(List<Runnable> threads) throws IOException {

        Result resOne = ((QueryThread) threads.get(0)).getResult();
        Result resTwo = ((QueryThread) threads.get(1)).getResult();
        Result resThree = ((QueryThread) threads.get(2)).getResult();

        List<Result> results = new ArrayList<Result>();
        results.add(resOne);
        results.add(resTwo);
        results.add(resThree);

        if (allEmpty(results)) {
            result = Result.EMPTY_RESULT;
        } else {
            CellVersionCheck check = sameVersionCells(results);
            LOG.debug("Check  result is " + check.isAllEqual());
            if (!check.isAllEqual()) {
                LOG.debug("Going to get missing results");
                List<Result> missingResults = getMissingResults(check);
                if (oneEmpty(missingResults)) {
                    //LOG.debug("Can't find missing results");
                    result = Result.EMPTY_RESULT;
                } else {
                    //LOG.debug("Returning complete results");
                    results = joinResults(results, missingResults, check);
                    //CellVersionCheck checkAgain = sameVersionCells(results);
                    //LOG.debug("Double check result is " + checkAgain.isAllEqual());
                    result = decodeResult(results);
                }

            } else {
                result = decodeResult(results);
            }
        }
    }

    private List<Result> joinResults(List<Result> originalResult, List<Result> missing, CellVersionCheck check) {
        List<Result> results = new ArrayList<Result>();

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < check.getPlayerstoGet().size(); j++) {
                if (check.getPlayerstoGet().get(j) == i) {
                    results.add(i, missing.get(j));
                } else {
                    results.add(originalResult.get(i));
                }
            }
        }
        return results;
    }

    private boolean allEmpty(List<Result> results) {
        boolean allEmpty = true;

        for (Result res : results) {
            allEmpty &= res.isEmpty();
        }
        return allEmpty;
    }

    private boolean oneEmpty(List<Result> results) {
        boolean oneEmpty = false;
        for (Result res : results) {
            oneEmpty |= res.isEmpty();
        }
        return oneEmpty;
    }

    public List<Result> getMissingResults(CellVersionCheck versionCheck) {
        List<Result> resultsCorrectVersion = new ArrayList<Result>();

        List<Runnable> calls = new ArrayList<Runnable>();
        List<Future> futures = new ArrayList<Future>();
        try {
            originalGet.setTimeStamp(versionCheck.getVersion());

            for (Integer i : versionCheck.getPlayerstoGet()) {
                HTable table = connections.get(i);
                calls.add(new GetThread(config, table, originalGet, true));
            }

            for (Runnable t : calls) {
                futures.add(threadPool.submit(t));
            }

            for (Future t : futures) {
                t.get();
            }

            for (Runnable t : calls) {
                resultsCorrectVersion.add(((QueryThread) t).getResult());

            }
        } catch (IOException | InterruptedException | ExecutionException e) {
            LOG.debug(e);
            throw new IllegalStateException(e);
        }
        return resultsCorrectVersion;
    }

    private CellVersionCheck sameVersionCells(List<Result> results) throws IOException {
        CellScanner firstScanner = results.get(0).cellScanner();
        CellScanner secondScanner = results.get(1).cellScanner();
        CellScanner thirdScanner = results.get(2).cellScanner();

        boolean valid = true;
        ArrayList<Long> getVersions = new ArrayList<>();
        while (firstScanner.advance() && secondScanner.advance()
                && thirdScanner.advance()) {
            Cell firstCell = firstScanner.current();
            Cell secondCell = secondScanner.current();
            Cell thirdCell = thirdScanner.current();
            long firstVersion = firstCell.getTimestamp();
            long secondVersion = secondCell.getTimestamp();
            long thirdVersion = thirdCell.getTimestamp();

            valid = firstVersion == secondVersion && secondVersion == thirdVersion;
            if (!valid) {
                LOG.debug("cell versions are " + firstVersion + " : " + secondVersion + " : " + thirdVersion);
                getVersions.add(firstVersion);
                getVersions.add(secondVersion);
                getVersions.add(thirdVersion);
                break;
            }
        }
        if (!valid) {
            Long versionToGet = Collections.max(getVersions);
            LOG.debug("Max Version is " + versionToGet);
            List<Integer> playersToGet = new ArrayList<Integer>();
            for (int i = 0; i < getVersions.size(); i++) {
                if (!getVersions.get(i).equals(versionToGet)) {
                    playersToGet.add(i);
                }
            }
            return new CellVersionCheck(false, playersToGet, versionToGet);
        } else {
            return new CellVersionCheck(true, null, 0L);
        }


    }

    @Override
    protected void joinThreads(List<Future> threads) throws IOException {
    }

    private class CellVersionCheck {

        private final boolean allEqual;
        private final List<Integer> playerstoGet;
        private final long version;

        public CellVersionCheck(boolean allEqual, List<Integer> playerstoGet, long version) {
            this.allEqual = allEqual;
            this.playerstoGet = playerstoGet;
            this.version = version;
        }

        public boolean isAllEqual() {
            return allEqual;
        }

        public List<Integer> getPlayerstoGet() {
            return playerstoGet;
        }

        public long getVersion() {
            return version;
        }

        @Override
        public String toString() {
            return "CellVersionCheck{" +
                    "allEqual=" + allEqual +
                    ", playerstoGet=" + playerstoGet.size() +
                    ", version=" + version +
                    '}';
        }
    }

}
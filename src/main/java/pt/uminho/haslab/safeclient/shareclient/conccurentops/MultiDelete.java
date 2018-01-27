package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import pt.uminho.haslab.safeclient.shareclient.SharedClientConfiguration;
import pt.uminho.haslab.safemapper.TableSchema;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class MultiDelete extends MultiOP {

    private Delete delete;

    private List<Delete> batchDelete;

    private boolean isBatchDelete;

    public MultiDelete(SharedClientConfiguration conf, List<HTable> connections, TableSchema schema, Delete delete, ExecutorService threadPool) {
        super(conf, connections, schema, threadPool);
        this.delete = delete;

    }

    public MultiDelete(SharedClientConfiguration conf, List<HTable> connections, TableSchema schema, List<Delete> delete, ExecutorService threadPool) {
        super(conf, connections, schema, threadPool);
        this.batchDelete = delete;
        isBatchDelete = true;
    }

    @Override
    protected Runnable queryThread(SharedClientConfiguration config, HTable table, int index) throws IOException {

        if (!isBatchDelete) {
            return new DeleteThread(config, table, delete);
        } else {
            return new DeleteThread(config, table, batchDelete);
        }
    }

    @Override
    protected void threadsJoined(List<Runnable> threads) throws IOException {
    }

    @Override
    protected void joinThreads(List<Future> threads) throws IOException {
        throw new UnsupportedOperationException("Join threads not supported for this operation.");
    }

    private class DeleteThread extends QueryThread {

        private Delete delete;

        private List<Delete> batchDelete;

        private boolean isBatchDelete;

        public DeleteThread(SharedClientConfiguration config, HTable table, Delete delete) {
            super(config, table);
            this.delete = delete;
        }

        public DeleteThread(SharedClientConfiguration config, HTable table, List<Delete> delete) {
            super(config, table);
            this.batchDelete = delete;
            isBatchDelete = true;
        }


        @Override
        protected void query() throws IOException {
            if (!isBatchDelete) {
                table.delete(delete);
            } else {
                table.delete(batchDelete);
            }
        }
    }
}

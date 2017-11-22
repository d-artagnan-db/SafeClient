package pt.uminho.haslab.safeclient.shareclient.conccurentops;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import pt.uminho.haslab.safeclient.shareclient.SharedClientConfiguration;
import pt.uminho.haslab.safemapper.TableSchema;

import java.io.IOException;
import java.util.List;

public class MultiDelete extends MultiOP {

    private Delete delete;

    private List<Delete> batchDelete;

    private boolean isBatchDelete;

    public MultiDelete(SharedClientConfiguration conf, List<HTable> connections, TableSchema schema, Delete delete) {
        super(conf, connections, schema);
        this.delete = delete;

    }

    public MultiDelete(SharedClientConfiguration conf, List<HTable> connections, TableSchema schema, List<Delete> delete) {
        super(conf, connections, schema);
        this.batchDelete = delete;
        isBatchDelete = true;
    }

    @Override
    protected Thread queryThread(SharedClientConfiguration config, HTable table, int index) throws IOException {

        if (!isBatchDelete) {
            return new DeleteThread(config, table, delete);
        } else {
            return new DeleteThread(config, table, batchDelete);
        }
    }

    @Override
    protected void threadsJoined(List<Thread> threads) throws IOException {

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


package pt.uminho.haslab.safecloudclient.shareclient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ehcache.Cache;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import pt.uminho.haslab.testingutils.ValuesGenerator;

import java.util.*;

@RunWith(Parameterized.class)
public class ClientCacheTest {
    protected static final int nThreads = 200;
    protected static final int nValues = 200;
    protected static final int nTables = 3;

    static final Log LOG = LogFactory.getLog(ClientCacheTest.class.getName());

    private final List<List<Long>> clientValues;
    private final List<String> tables;
    private final ClientCache cache;

    public ClientCacheTest(List<List<Long>> clientValues, List<String> tables){
        this.clientValues = clientValues;
        this.tables = tables;
        this.cache = new ClientCacheImpl(5);
    }

    /**
     * Each thread will have multiple numbers that will be inserted on the
     * cache. These numbers work as identifier and row.
     * Values can be inserted into different tables.
     *
     * @return  */
    @Parameterized.Parameters
    public static Collection valueGenerator() {
        return ValuesGenerator.ClientCacheGenerator(nValues, nThreads, nTables);
    }
    
    @Test
    public void test() throws InterruptedException {

        List<Thread> threads =  new ArrayList<Thread>();
        Random randomGenerator = new Random();
        for(int i=0; i < nThreads; i++){
            int pos  = randomGenerator.nextInt(tables.size());
            String tableName = tables.get(pos);
            LOG.debug("Creating client thread for table "+ tableName);

            Thread t = new Client(tableName, clientValues.get(i));
            threads.add(t);
        }

        for(Thread t: threads){
            LOG.debug("Starting client threads");
            t.start();
        }

        for(Thread t: threads){
            LOG.debug("Waiting on threads to finish");
            t.join();
        }

        for(Thread t: threads){
            LOG.debug("Validating clients execution");
            Assert.assertEquals(true, ((Client)t).getPassedTest());
        }
        /**
         * Check if the values stored in the cache were insert by one of the
         * clients
         */
        boolean allVerified = true;
        for(String table: tables){

            ClientCacheImpl cacheimpl = (ClientCacheImpl)  cache;


            Iterator<Cache.Entry<byte[],Long>> it = cacheimpl.iterateCache(table);
            boolean found = false;
            while(it.hasNext()){
                Cache.Entry<byte[], Long> entry = it.next();
                LOG.debug("Searching on table "+ table +" the row "+ new String(entry.getKey()) + " with val "+ entry.getValue());

                for(Thread t: threads){
                    Client cli = (Client) t;
                    if(cli.getTableName().equals(table)) {
                        LOG.debug("Found Match");
                        found |= cli.insertedEntry(entry.getKey(), entry.getValue());
                    }
                }
            }
            allVerified &= found;
        }
        Assert.assertEquals(true, allVerified);

    }

    private class Client extends Thread{

        private final String tableName;
        private final List<Long> values;
        private boolean passedTest;
        private int nOps = 0;
        private final Map<byte[], Long> insertedValues;

        public Client(String tableName, List<Long> values){
            this.tableName = tableName;
            this.values = values;
            passedTest = false;
            insertedValues = new HashMap<byte[], Long>();

        }

        public boolean getPassedTest(){
            return passedTest;
        }

        public boolean insertedEntry(byte[] row, Long val){
            return insertedValues.containsKey(row) && insertedValues.get(row).equals(val);
        }

        public String getTableName(){
            return this.tableName;
        }

        public void run(){

            for(int i=0; i <  values.size(); i++){
                LOG.debug("Going to put on table " + tableName + " the key " + i + " with value " + values.get(i));
                byte[] key = (""+i).getBytes();
                cache.put(tableName, key, values.get(i));
                insertedValues.put(key, values.get(i));
                if(cache.containsKey(tableName, key)){
                    cache.get(tableName, key);
                }
                nOps+=1;
            }
            /**
             * Each client does a simple test. It simply tests if it has
             * made every operation by counting  the number of ops done in the
             * loop.
             * The reason for such a simple test is because the unit tests
             * launches that may write to the same table and override values,
             * which makes it hard to predict which values will be stored
             * on the final cache what should be the correct behavior.
             * This test simply allows to find exception or wrong behavior
             * easier.
             * */
            if(nOps == values.size()){
                LOG.debug("All ops concluded");
                passedTest = true;
            }
        }

    }
}

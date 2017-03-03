package pt.uminho.haslab.safecloudclient.shareclient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;


public class ClientCacheImpl implements ClientCache {
    static final Log LOG = LogFactory.getLog(ClientCacheImpl.class.getName());

    private final CacheManager cacheManager;
        
    private final RowLocks locks;
    private final int heapSize;

    public ClientCacheImpl(int heapSize){
        cacheManager = CacheManagerBuilder
                .newCacheManagerBuilder()
                .build();
        cacheManager.init();
        locks = new RowLocksImpl();
        this.heapSize = heapSize;
    }


    public void put(String tableName, byte[] row, Long uniqueID) {
        LOG.debug("Going to put value on cache "+ new BigInteger(row));
        Lock lock = locks.writeLock(tableName, row);
        lock.lock();
        try {
            synchronized (this){
                LOG.debug("Going to get table cache");
                /**row[] cannot be used as a Map key as it uses the object
                 *identity for comparision and does
                 *not compare the array content. Thus two arrays with the same
                 *content but with different references are not equal.
                 */
                Cache<BigInteger, Long> keysCache = cacheManager.getCache(tableName, BigInteger.class, Long.class);
                if(keysCache == null){
                    keysCache = cacheManager.createCache(tableName,
                                    newCacheConfigurationBuilder(BigInteger.class,
                                                      Long.class,
                                                      heap(heapSize)).build());
                }
            }
            Cache<BigInteger, Long> keysCache = cacheManager.getCache(tableName, BigInteger.class, Long.class);
            keysCache.put(new BigInteger(row), uniqueID);
            LOG.debug("Inserted value onc cache");
        } finally {
            lock.unlock();
        }
    }

    public boolean containsKey(String tableName, byte[] key) {
        Cache<BigInteger, Long> keysCache = cacheManager.getCache(tableName,
                                                              BigInteger.class,
                                                              Long.class);
        LOG.debug("Checking if row exists in cache "+ keysCache);
        if(keysCache != null){
            LOG.debug("Exits? "+ keysCache.containsKey(new BigInteger(key)));
        }
        return keysCache != null && keysCache.containsKey(new BigInteger(key));
    }

    /**
     * This function assumes that a check with containsKey has been made by
     * the class client.
     * 
     */
    public Long get(String tableName, byte[] row) {
        Lock lock = locks.readLock(tableName, row);
        lock.lock();
        Long val;
        try {
             Cache<BigInteger, Long> keysCache = cacheManager.getCache(tableName, BigInteger.class, Long.class);
             val = keysCache.get(new BigInteger(row));
        } finally {
            lock.unlock();
        }
        return val;
    }

    // Function to be used only on UnitTests
    public Iterator<Cache.Entry<byte[],Long>> iterateCache(String tableName){
        Cache<byte[], Long> keysCache = cacheManager.getCache(tableName,
                byte[].class,
                Long.class);

        return keysCache.iterator();
    }
}

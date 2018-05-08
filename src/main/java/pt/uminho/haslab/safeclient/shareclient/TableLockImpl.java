package pt.uminho.haslab.safeclient.shareclient;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TableLockImpl implements TableLock {

    private final Map<String, ReadWriteLock> locks;

    public TableLockImpl() {
        locks = new HashMap<String, ReadWriteLock>();
    }

    private ReadWriteLock getLock(String tableName) {

        if (!locks.containsKey(tableName)) {
            locks.put(tableName, new ReentrantReadWriteLock());
        }
        return locks.get(tableName);

    }

    public synchronized Lock readLock(String tableName) {
        return getLock(tableName).readLock();
    }

    public synchronized Lock writeLock(String tableName) {
        return getLock(tableName).writeLock();
    }
}

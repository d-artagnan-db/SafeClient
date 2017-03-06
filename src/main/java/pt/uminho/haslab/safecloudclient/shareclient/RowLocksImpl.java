package pt.uminho.haslab.safecloudclient.shareclient;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RowLocksImpl implements RowLocks {

	private final Map<String, Map<byte[], ReadWriteLock>> locks;

	public RowLocksImpl() {

		locks = new HashMap<String, Map<byte[], ReadWriteLock>>();
	}

	private ReadWriteLock getLock(String tableName, byte[] row) {

		if (!locks.containsKey(tableName)) {
			Map<byte[], ReadWriteLock> tLocks = new HashMap<byte[], ReadWriteLock>();
			locks.put(tableName, tLocks);
		}

		if (locks.containsKey(tableName)
				&& !locks.get(tableName).containsKey(row)) {
			Map<byte[], ReadWriteLock> tLocks = locks.get(tableName);
			ReadWriteLock lock = new ReentrantReadWriteLock(true);
			tLocks.put(row, lock);
		}

		return locks.get(tableName).get(row);
	}

	public synchronized Lock readLock(String tableName, byte[] row) {
		return getLock(tableName, row).readLock();
	}

	public synchronized Lock writeLock(String tableName, byte[] row) {
		return getLock(tableName, row).writeLock();
	}
}

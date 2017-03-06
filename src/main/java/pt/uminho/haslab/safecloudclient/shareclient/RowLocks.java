package pt.uminho.haslab.safecloudclient.shareclient;

import java.util.concurrent.locks.Lock;

public interface RowLocks {

	public Lock readLock(String tableName, byte[] row);

	public Lock writeLock(String tableName, byte[] row);
}

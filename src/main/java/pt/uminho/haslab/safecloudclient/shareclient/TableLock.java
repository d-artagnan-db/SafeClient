package pt.uminho.haslab.safecloudclient.shareclient;

import java.util.concurrent.locks.Lock;

public interface TableLock {

	public Lock readLock(String tableName);

	public Lock writeLock(String tableName);
}

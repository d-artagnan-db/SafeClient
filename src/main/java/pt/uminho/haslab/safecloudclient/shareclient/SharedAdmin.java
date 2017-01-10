package pt.uminho.haslab.safecloudclient.shareclient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SharedAdmin {

	static final Log LOG = LogFactory.getLog(SharedAdmin.class.getName());

	private final List<HBaseAdmin> admins;
	private final List<Configuration> confs;
	private SharedClientConfiguration sharedConfig;

	public SharedAdmin(Configuration conf) throws ZooKeeperConnectionException,
			IOException {
		admins = new ArrayList<HBaseAdmin>();
		confs = new ArrayList<Configuration>();
		for (int i = 1; i < 4; i++) {
			/**
			 * 
			 * This is not the best approach to handle the configurations.
			 */
			sharedConfig = new SharedClientConfiguration(conf, i);
			Configuration clusterConfig = sharedConfig
					.createClusterConfiguration();
			confs.add(clusterConfig);
			HBaseAdmin admin = new HBaseAdmin(clusterConfig);
			admins.add(admin);
			// System.out.println("Going to request tables");
			// System.out.println(Arrays.toString(admin.getConnection()
			// .listTableNames()));
		}

	}

	public void createTable(final HTableDescriptor descriptor)
			throws IOException, InterruptedException {

		// Add the extra column to store the sharemind secrets.
		String family = sharedConfig.getShareKeyColumnFamily();
		HColumnDescriptor familyDesc = new HColumnDescriptor(family);
		descriptor.addFamily(familyDesc);
		List<Thread> threads = new ArrayList<Thread>();
		for (final HBaseAdmin admin : admins) {
			Thread t = new Thread() {

				@Override
				public void run() {
					try {
						System.out.println("Going to create a table");
						admin.createTable(descriptor);
						System.out.println("Table created");
					} catch (IOException ex) {
						System.out.println(ex);
					}

				}
			};

			threads.add(t);
			t.start();
		}

		for (Thread t : threads) {
			t.join();
		}

	}

	public void deleteTable(String tableName) throws IOException {

		for (HBaseAdmin admin : admins) {

			admin.deleteTable(tableName);
		}

	}

	public void disableTable(String tableName) throws IOException {

		for (HBaseAdmin admin : admins) {
			admin.disableTable(tableName);
		}

	}

	public boolean tableExits(String tableName) throws IOException {

		boolean tablesExist = true;

		for (HBaseAdmin admin : admins) {
			tablesExist &= admin.tableExists(tableName);
		}

		return tablesExist;

	}

	public void close() throws IOException {

		for (HBaseAdmin admin : admins) {
			admin.close();
		}
	}

}

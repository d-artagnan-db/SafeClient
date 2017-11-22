package pt.uminho.haslab.safeclient.shareclient;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import pt.uminho.haslab.safeclient.CHBaseAdmin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SharedAdmin implements CHBaseAdmin {

	static final Log LOG = LogFactory.getLog(SharedAdmin.class.getName());

	private final List<HBaseAdmin> admins;
	private final List<Configuration> confs;
	private SharedClientConfiguration sharedConfig;

    public SharedAdmin(Configuration conf) throws IOException {
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
		}

	}

	public void createTable(final HTableDescriptor descriptor)
			throws IOException, InterruptedException {

		List<Thread> threads = new ArrayList<Thread>();
		for (final HBaseAdmin admin : admins) {
			Thread t = new Thread() {

				@Override
				public void run() {
					try {
						admin.createTable(descriptor);
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

	public boolean tableExists(String tableName) throws IOException {

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

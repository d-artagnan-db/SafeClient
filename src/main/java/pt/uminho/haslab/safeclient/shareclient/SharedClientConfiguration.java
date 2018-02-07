package pt.uminho.haslab.safeclient.shareclient;

import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SharedClientConfiguration {

	private final Configuration conf;
	private final int id;

	private final boolean hasConcurrentScanEndpoint;
	private final int scanQueueSize;
	private final int getVersionNRetries;
	private final int getVersionSleep;
	private final int getVersionBackoff;

	public SharedClientConfiguration(Configuration conf, int id) {
		this.conf = conf;
		this.id = id;
		hasConcurrentScanEndpoint = conf.getBoolean("saferegions.coprocessor.concurrent", false);
		scanQueueSize = conf.getInt("sharedClient.scan.queue.size", 100);
		getVersionNRetries = conf.getInt("sharedClient.get.version.retries", 10);
		getVersionSleep = conf.getInt("sharedClient.get.version.sleep", 100);
		getVersionBackoff = conf.getInt("sharedClient.get.version.backoff", 100);

	}

	public int getScanQueueSize() {
		return scanQueueSize;
	}

	public boolean hasConcurrentScanEndpoint() {
        return hasConcurrentScanEndpoint;
    }

    public Configuration createClusterConfiguration() {
		Configuration cluster = new Configuration();
        List<String> keys = new ArrayList<String>();
        List<String> values = new ArrayList<String>();

		for (Map.Entry<String, String> entry : conf) {
			String key = entry.getKey();
			String value = entry.getValue();

			if (key.contains("cluster" + id)) {

				String clusterKey = key.replace("cluster" + id + ".", "");
                keys.add(clusterKey);
                values.add(value);
            } else if (!key.contains("cluster")) {
                cluster.set(key, value);
			}
		}
        for (int i = 0; i < keys.size(); i++) {
            cluster.set(keys.get(i), values.get(i));
        }
        return cluster;
	}

	public int getGetVersionNRetries() {
		return getVersionNRetries;
	}

	public int getGetVersionSleep() {
		return getVersionSleep;
	}

	public int getGetVersionBackoff() {
		return getVersionBackoff;
	}
}

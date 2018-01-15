package pt.uminho.haslab.safeclient.shareclient;

import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SharedClientConfiguration {

	private final Configuration conf;
	private final int id;

	private final boolean hasConcurrentScanEndpoint;

	public SharedClientConfiguration(Configuration conf, int id) {
		this.conf = conf;
		this.id = id;
		hasConcurrentScanEndpoint = conf.getBoolean("saferegions.coprocessor.concurrent", false);

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
}

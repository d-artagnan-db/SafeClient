package pt.uminho.haslab.safeclient.shareclient;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public class SharedClientConfiguration {

	private final Configuration conf;
	private final int id;

	public SharedClientConfiguration(Configuration conf, int id) {
		this.conf = conf;
		this.id = id;

	}

	public Configuration createClusterConfiguration() {
		Configuration cluster = new Configuration();

		for (Map.Entry<String, String> entry : conf) {
			String key = entry.getKey();
			String value = entry.getValue();
			if (key.contains("cluster" + id)) {
				String clusterKey = key.replace("cluster" + id + ".", "");
				cluster.set(clusterKey, value);
			} else {
				cluster.set(key, value);
			}
		}
		return cluster;
	}
}

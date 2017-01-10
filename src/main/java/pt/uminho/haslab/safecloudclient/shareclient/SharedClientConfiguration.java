package pt.uminho.haslab.safecloudclient.shareclient;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;

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
	public String getShareKeyColumnFamily() {
		return conf.get("smhbase.column.family");
	}

	public String getShareKeyColumnQualifier() {
		return conf.get("smhbase.column.qualifier");
	}

	public int getNBits() {
		return conf.getInt("smhbase.nbits", -1);
	}

}

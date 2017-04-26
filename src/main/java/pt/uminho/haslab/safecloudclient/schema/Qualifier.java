package pt.uminho.haslab.safecloudclient.schema;

import pt.uminho.haslab.cryptoenv.CryptoTechnique;

import java.util.HashMap;
import java.util.Map;

/**
 * Qualifier class.
 * Holds all the relevant information associated to a specific Qualifier.
 */
public class Qualifier {

	private String qualifierName;
	private CryptoTechnique.CryptoType cryptoType;
	private int formatSize;
	private Map<String, String> properties;

	public Qualifier() {
		this.qualifierName = "";
		this.cryptoType = CryptoTechnique.CryptoType.PLT;
		this.formatSize = 0;
		this.properties = new HashMap<>();
	}

	public Qualifier(String name, CryptoTechnique.CryptoType cType, int format, Map<String, String> properties) {
		this.qualifierName = name;
		this.cryptoType = cType;
		this.formatSize = format;
		this.properties = properties;
	}

	public String getName() {
		return this.qualifierName;
	}

	public CryptoTechnique.CryptoType getCryptoType() {
		return this.cryptoType;
	}

	public int getFormatSize() {
		return this.formatSize;
	}

	public void setQualifierName(String name) {
		this.qualifierName = name;
	}

	public void setCryptoType(CryptoTechnique.CryptoType cryptoType) {
		this.cryptoType = cryptoType;
	}

	public void setFormatSize(int format) {
		this.formatSize = format;
	}

	public Map<String,String> getProperties() {
		Map<String, String> propertiesTemp = new HashMap<>();
		for(String s : this.properties.keySet()) {
			propertiesTemp.put(s, this.properties.get(s));
		}
		return propertiesTemp;
	}

	public void setProperties(Map<String, String> prop) {
		this.properties = new HashMap<>();
		for(String s : prop.keySet()) {
			this.properties.put(s, prop.get(s));
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Qualifier Name: ").append(qualifierName).append("\n");
		sb.append("Qualifier CryptoType: ").append(cryptoType).append("\n");
		sb.append("Qualifier Format Size: ").append(formatSize).append("\n");
		sb.append("Qualifier Properties: \n");
		for(String s : this.properties.keySet()) {
			sb.append(s).append(": ").append(this.properties.get(s)).append("\n");
		}
		return sb.toString();
	}

}

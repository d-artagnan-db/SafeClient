package pt.uminho.haslab.safecloudclient.schema;

import pt.uminho.haslab.cryptoenv.CryptoTechnique;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Qualifier class.
 * Holds all the relevant information associated to a specific Qualifier.
 */
public class Qualifier {

	private byte[] qualifierName;
	private CryptoTechnique.CryptoType cryptoType;
	private int formatSize;
	private Boolean padding;
	private Map<String, String> properties;

	public Qualifier() {
		this.qualifierName = null;
		this.cryptoType = CryptoTechnique.CryptoType.PLT;
		this.formatSize = 0;
		this.padding = null;
		this.properties = new HashMap<>();
	}

	public Qualifier(byte[] name, CryptoTechnique.CryptoType cType, int format, Boolean padding, Map<String, String> properties) {
//		this.qualifierName = name;
		this.qualifierName = Arrays.copyOf(name, name.length);
		this.cryptoType = cType;
		this.formatSize = format;
		this.padding = padding;
		this.properties = properties;
	}

	public byte[] getName() {
		return this.qualifierName;
	}

	public CryptoTechnique.CryptoType getCryptoType() {
		return this.cryptoType;
	}

	public int getFormatSize() {
		return this.formatSize;
	}

	public Boolean getPadding() {
		return this.padding;
	}

	public void setQualifierName(byte[] name) {
		this.qualifierName = Arrays.copyOf(name, name.length);
	}

	public void setCryptoType(CryptoTechnique.CryptoType cryptoType) {
		this.cryptoType = cryptoType;
	}

	public void setFormatSize(int format) {
		this.formatSize = format;
	}

	public void setPadding(Boolean padding) {
		this.padding = padding;
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
		sb.append("Qualifier Name: ").append(Arrays.toString(qualifierName)).append("\n");
		sb.append("Qualifier CryptoType: ").append(cryptoType).append("\n");
		sb.append("Qualifier Format Size: ").append(formatSize).append("\n");
		sb.append("Qualifier Padding: ").append(padding).append("\n");
		sb.append("Qualifier Properties: \n");
		for(String s : this.properties.keySet()) {
			sb.append(s).append(": ").append(this.properties.get(s)).append("\n");
		}
		return sb.toString();
	}

}

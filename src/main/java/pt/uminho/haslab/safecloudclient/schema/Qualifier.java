package pt.uminho.haslab.safecloudclient.schema;

import pt.uminho.haslab.cryptoenv.CryptoTechnique;

/**
 * Created by rgmacedo on 3/13/17.
 */
public class Qualifier {

	private String qualifierName;
	private CryptoTechnique.CryptoType cryptoType;
	private int formatSize;

	public Qualifier() {
		this.qualifierName = "";
		this.cryptoType = CryptoTechnique.CryptoType.STD;
		this.formatSize = 0;
	}

	public Qualifier(String name, CryptoTechnique.CryptoType cType, int format) {
		this.qualifierName = name;
		this.cryptoType = cType;
		this.formatSize = format;
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

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Qualifier Name: ").append(qualifierName).append("\n");
		sb.append("Qualifier CryptoType: ").append(cryptoType).append("\n");
		sb.append("Qualifier Format Size: ").append(formatSize).append("\n");
		return sb.toString();
	}

}

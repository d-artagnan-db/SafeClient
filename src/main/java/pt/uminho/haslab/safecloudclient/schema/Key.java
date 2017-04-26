package pt.uminho.haslab.safecloudclient.schema;

import pt.uminho.haslab.cryptoenv.CryptoTechnique;

/**
 * Key class.
 * Holds all the relevant information associated to the row key
 */
public class Key {

	private CryptoTechnique.CryptoType cryptoType;
	private int formatSize;

	public Key() {
		this.cryptoType = CryptoTechnique.CryptoType.PLT;
		this.formatSize = 0;
	}

	public Key(CryptoTechnique.CryptoType cType, int formatSize) {
		this.cryptoType = cType;
		this.formatSize = formatSize;
	}

	public CryptoTechnique.CryptoType getCryptoType() {
		return this.cryptoType;
	}

	public int getFormatSize() {
		return this.formatSize;
	}

	public void setCryptoType(CryptoTechnique.CryptoType cryptoType) {
		this.cryptoType = cryptoType;
	}

	public void setFormatSize(int format) {
		this.formatSize = format;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Key [").append(this.cryptoType).append(", ")
				.append(this.formatSize).append("]\n");
		return sb.toString();
	}
}

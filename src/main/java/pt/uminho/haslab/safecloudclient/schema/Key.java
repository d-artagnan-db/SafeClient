package pt.uminho.haslab.safecloudclient.schema;

import pt.uminho.haslab.cryptoenv.CryptoTechnique;

/**
 * Key class.
 * Holds all the relevant information associated to the Row-Key
 */
public class Key {

	private CryptoTechnique.CryptoType cryptoType;
	private int formatSize;
	private Boolean keyPadding;

	public Key() {
		this.cryptoType = CryptoTechnique.CryptoType.PLT;
		this.formatSize = 0;
		this.keyPadding = null;
	}

	public Key(CryptoTechnique.CryptoType cType, int formatSize, Boolean padding) {
		this.cryptoType = cType;
		this.formatSize = formatSize;
		this.keyPadding = padding;
	}

	public CryptoTechnique.CryptoType getCryptoType() {
		return this.cryptoType;
	}

	public int getFormatSize() {
		return this.formatSize;
	}

	public Boolean getKeyPadding() {
		return this.keyPadding;
	}

	public void setCryptoType(CryptoTechnique.CryptoType cryptoType) {
		this.cryptoType = cryptoType;
	}

	public void setFormatSize(int format) {
		this.formatSize = format;
	}

	public void setKeyPadding(Boolean padding) {
		this.keyPadding = padding;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Key [").append(this.cryptoType).append(", ")
				.append(this.formatSize).append(", ")
				.append(this.keyPadding).append("]\n");
		return sb.toString();
	}
}

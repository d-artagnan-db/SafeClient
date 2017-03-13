package pt.uminho.haslab.safecloudclient.schema;

import pt.uminho.haslab.cryptoenv.CryptoTechnique;

/**
 * Created by rgmacedo on 3/13/17.
 */
public class Key {

    public CryptoTechnique.CryptoType cryptoType;
    public int formatSize;

    public Key() {
        this.cryptoType = CryptoTechnique.CryptoType.STD;
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
        sb.append("Key [").append(this.cryptoType).append(", ").append(this.formatSize).append("]\n");
        return sb.toString();
    }
}

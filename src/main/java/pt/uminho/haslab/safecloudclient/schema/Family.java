package pt.uminho.haslab.safecloudclient.schema;

import org.apache.commons.codec.digest.Crypt;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rgmacedo on 3/13/17.
 */
public class Family {

    private String familyName;
    private CryptoTechnique.CryptoType cryptoType;
    private int formatSize;
    private List<Qualifier> qualifiers;

    public Family() {
        this.familyName = "";
        this.cryptoType = CryptoTechnique.CryptoType.STD;
        this.formatSize = 0;
        this.qualifiers = new ArrayList<Qualifier>();
    }

    public Family(String familyName, CryptoTechnique.CryptoType cType, int formatSize, List<Qualifier> quals) {
        this.familyName = familyName;
        this.cryptoType = cType;
        this.formatSize = formatSize;
        this.qualifiers = quals;
    }

    public String getFamilyName() {
        return this.familyName;
    }

    public CryptoTechnique.CryptoType getCryptoType() {
        return this.cryptoType;
    }

    public int getFormatSize() {
        return this.formatSize;
    }

    public List<Qualifier> getQualifiers() {
        List<Qualifier> qualifiersTemp = new ArrayList<Qualifier>();
        for(Qualifier q : this.qualifiers)
            qualifiersTemp.add(q);
        return qualifiersTemp;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public void setCryptoType(CryptoTechnique.CryptoType cType) {
        this.cryptoType = cType;
    }

    public void setFormatSize(int formatSize) {
        this.formatSize = formatSize;
    }

    public void setQualifiers(List<Qualifier> qualifiers) {
        this.qualifiers = new ArrayList<Qualifier>();
        for(Qualifier q : qualifiers) {
            this.qualifiers.add(q);
        }
    }

    /**
     * Add a new qualifier to the Qualifiers list. If both format size and cryptoType is not defined, the qualifier
     * assume both values from is parent, the column family.
     * @param qualifierName
     * @param cryptoType
     * @param formatSize
     */
    public void addQualifier(String qualifierName, CryptoTechnique.CryptoType cryptoType, int formatSize) {
        CryptoTechnique.CryptoType cType;
        int fSize = 0;

        if(cryptoType != null)
            cType = cryptoType;
        else
            cType = this.cryptoType;

        if(formatSize > 0)
            fSize = formatSize;
        else
            fSize = this.formatSize;

        this.qualifiers.add(new Qualifier(qualifierName, cryptoType, formatSize));
    }

    public void addQualifier(Qualifier qualifier) {
        if(qualifier.getCryptoType() == null)
            qualifier.setCryptoType(this.cryptoType);

        if(qualifier.getFormatSize() == 0)
            qualifier.setFormatSize(this.formatSize);

        this.qualifiers.add(qualifier);
    }
}

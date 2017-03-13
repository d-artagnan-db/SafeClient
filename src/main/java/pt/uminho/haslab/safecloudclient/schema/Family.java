package pt.uminho.haslab.safecloudclient.schema;

import org.apache.commons.codec.digest.Crypt;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by rgmacedo on 3/13/17.
 */
public class Family {

    public String familyName;
    public CryptoTechnique.CryptoType cryptoType;
    public int formatSize;
    public List<Qualifier> qualifiers;

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


}

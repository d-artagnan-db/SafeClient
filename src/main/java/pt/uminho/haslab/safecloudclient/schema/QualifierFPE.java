package pt.uminho.haslab.safecloudclient.schema;

import pt.uminho.haslab.cryptoenv.CryptoTechnique;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties.getTweakBytes;
import static pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties.whichFpeInstance;

/**
 * Created by rgmacedo on 5/4/17.
 */
public class QualifierFPE extends Qualifier {
    private byte[] qualifierName;
    private CryptoTechnique.CryptoType cryptoType;
    private int formatSize;
    private Boolean padding;
    private Map<String, String> properties;
    private String instance;
    private CryptoTechnique.FFX fpe_instance;
    private int radix;
    private String tweak;

    public QualifierFPE() {
        this.qualifierName = null;
        this.cryptoType = CryptoTechnique.CryptoType.FPE;
        this.formatSize = 0;
        this.padding = null;
        this.properties = new HashMap<>();
        this.instance = "FF1";
        this.fpe_instance = CryptoTechnique.FFX.FF1;
        this.radix = 10;
        this.tweak = "";
    }

    public QualifierFPE(byte[] qualifierName, CryptoTechnique.CryptoType cType, int formatSize, Boolean padding, Map<String,String> prop, String instance, int radix, String tweak) {
        super(qualifierName,cType,formatSize,padding,prop);
        this.instance = instance;
        this.fpe_instance = whichFpeInstance(instance);
        this.radix = radix;
        this.tweak = tweak;
    }

    public String getInstance() {
        return this.instance;
    }

    public CryptoTechnique.FFX getFpeInstance() {
        return this.fpe_instance;
    }

    public int getRadix() {
        return this.radix;
    }

    public String getTweak() {
        return this.tweak;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public void setFpeInstance(CryptoTechnique.FFX instance) {
        this.fpe_instance = instance;
    }

    public void setRadix(int radix) {
        this.radix = radix;
    }

    public void setTweak(String tweak) {
        this.tweak = tweak;
    }

    public byte[] getSecurityParameters(byte[] key) {
        byte[] temp_tweak = getTweakBytes(this.instance, this.tweak);
        byte[] security_parameters = new byte[key.length + temp_tweak.length];
        System.arraycopy(key, 0, security_parameters, 0, key.length);
        System.arraycopy(temp_tweak, 0, security_parameters, key.length, temp_tweak.length);
        return security_parameters;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Qualifier instance: ").append(this.instance).append("\n");
        sb.append("Qualifier radix: ").append(this.radix).append("\n");
        sb.append("Qualifier tweak: ").append(this.tweak).append("\n");
        return super.toString()+sb.toString();
    }

}

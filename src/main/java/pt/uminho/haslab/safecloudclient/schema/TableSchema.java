package pt.uminho.haslab.safecloudclient.schema;

import com.sun.tools.javac.util.Name;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rgmacedo on 3/13/17.
 */
public class TableSchema {
//    The name of the table
    private String tablename;
//    Default values
    private CryptoTechnique.CryptoType defaultKeyCryptoType;
    private CryptoTechnique.CryptoType defaultColumnsCryptoType;
    private int defaultFormatSize;

//    The CryptoTechnique to use in the keys
    private Key key;
//    The Column Families and the respective Column Qualifiers and CryptoTechniques
    private List<Family> columnFamilies;

    public TableSchema() {
        this.tablename = "";
        this.defaultKeyCryptoType = CryptoTechnique.CryptoType.STD;
        this.defaultColumnsCryptoType = CryptoTechnique.CryptoType.STD;
        this.defaultFormatSize = 0;
        this.key = null;
        this.columnFamilies = new ArrayList<Family>();
    }

    public TableSchema(String tablename, CryptoTechnique.CryptoType defKey, CryptoTechnique.CryptoType defColumns, int defFormat, Key key, List<Family> families) {
        this.tablename = tablename;
        this.defaultKeyCryptoType = defKey;
        this.defaultColumnsCryptoType = defColumns;
        this.defaultFormatSize = defFormat;
        this.key = key;
        this.columnFamilies = families;
    }

    public String getTablename() {
        return this.tablename;
    }

    public CryptoTechnique.CryptoType getDefaultKeyCryptoType() {
        return this.defaultKeyCryptoType;
    }

    public CryptoTechnique.CryptoType getDefaultColumnsCryptoType() {
        return this.defaultColumnsCryptoType;
    }

    public int getDefaultFormatSize() {
        return this.defaultFormatSize;
    }

    public Key getKey() {
        return this.key;
    }

    public List<Family> getColumnFamilies() {
        List<Family> tempFamilies = new ArrayList<Family>();

        for(Family family : this.columnFamilies)
            tempFamilies.add(family);

        return tempFamilies;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public void setDefaultKeyCryptoType(CryptoTechnique.CryptoType cType) {
        this.defaultKeyCryptoType = cType;
    }

    public void setDefaultColumnsCryptoType(CryptoTechnique.CryptoType cType) {
        this.defaultColumnsCryptoType = cType;
    }

    public void setDefaultFormatSize(int formatSize) {
        this.defaultFormatSize = formatSize;
    }

    public void setKey(Key key) {
        this.key = key;
    }

    public void setColumnFamilies(List<Family> families) {
        this.columnFamilies = new ArrayList<Family>();

        for(Family family : families)
            this.columnFamilies.add(family);
    }


    public void addFamily(String familyName, CryptoTechnique.CryptoType cType, int formatSize, List<Qualifier> qualifiers) {
        Family family = new Family();
        family.setFamilyName(familyName);

        if(cType == null)
            family.setCryptoType(defaultColumnsCryptoType);
        else
            family.setCryptoType(cType);

        if(formatSize <= 0)
            family.setFormatSize(defaultFormatSize);
        else
            family.setFormatSize(formatSize);

        for(Qualifier q : qualifiers)
            family.addQualifier(q);

        this.columnFamilies.add(family);
    }

    public void addFamily(Family fam) {
        if(fam.getCryptoType() == null)
            fam.setCryptoType(defaultColumnsCryptoType);

        if(fam.getFormatSize() <= 0)
            fam.setFormatSize(defaultFormatSize);

        this.columnFamilies.add(fam);
    }

//    public void addQualifier(String familyName, Qualifier q) {
//
//    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Table Schema\n");
        sb.append("Table Name: ").append(this.tablename).append("\n");
        sb.append("Key CryptoType: ").append(this.key.toString()).append("\n");
        sb.append("Columns: \n");
        for(Family family : this.columnFamilies) {
            sb.append("> Family: ").append(family.toString()).append("\n");
        }

        return sb.toString();
    }



}

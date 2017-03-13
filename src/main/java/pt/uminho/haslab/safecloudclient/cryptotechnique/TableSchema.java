package pt.uminho.haslab.safecloudclient.cryptotechnique;

import com.sun.tools.javac.util.Name;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rgmacedo on 3/13/17.
 */
public class TableSchema {
//    The name of the table
    private String tablename;
//    The CryptoTechnique to use in the keys
    private CryptoTechnique.CryptoType key;
//    The Column Families and the respective Column Qualifiers and CryptoTechniques
    private Map<String, Map<String, CryptoTechnique.CryptoType>> columnFamilies;

    public TableSchema() {
        this.tablename = "";
        this.key = null;
        this.columnFamilies = new HashMap<String, Map<String, CryptoTechnique.CryptoType>>();
    }

    public TableSchema(String tablename, CryptoTechnique.CryptoType key, Map<String, Map<String, CryptoTechnique.CryptoType>> families) {
        this.tablename = tablename;
        this.key = key;
        this.columnFamilies = families;
    }

    public String getTablename() {
        return this.tablename;
    }

    public CryptoTechnique.CryptoType getKey() {
        return this.key;
    }

    public Map<String, Map<String, CryptoTechnique.CryptoType>> getColumnFamilies() {
        Map<String, Map<String, CryptoTechnique.CryptoType>> tempFamilies = new HashMap<String, Map<String, CryptoTechnique.CryptoType>>();

        for(String family : this.columnFamilies.keySet()) {
            Map<String, CryptoTechnique.CryptoType> tempQualifiers = new HashMap<String, CryptoTechnique.CryptoType>();
            for(String qualifier : this.columnFamilies.get(family).keySet()) {
                tempQualifiers.put(qualifier, this.columnFamilies.get(family).get(qualifier));
            }
            tempFamilies.put(family, tempQualifiers);
        }

        return tempFamilies;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public void setKey(CryptoTechnique.CryptoType key) {
        this.key = key;
    }

    public void setColumnFamilies(Map<String, Map<String, CryptoTechnique.CryptoType>> families) {
        this.columnFamilies = new HashMap<String, Map<String, CryptoTechnique.CryptoType>>();

        for(String family : families.keySet()) {
            Map<String, CryptoTechnique.CryptoType> qualifiers = new HashMap<String, CryptoTechnique.CryptoType>();
            for(String qualifier : families.get(family).keySet()) {
                qualifiers.put(qualifier, families.get(family).get(qualifier));
            }
            this.columnFamilies.put(family, qualifiers);
        }
    }

    public void addFamily(String family) {
        if (!this.columnFamilies.containsKey(family)) {
            this.columnFamilies.put(family, new HashMap<String, CryptoTechnique.CryptoType>());
        }
    }

    public void addFamily(String family, Map<String, CryptoTechnique.CryptoType> qualifiers) {
        if (!this.columnFamilies.containsKey(family)) {
            this.columnFamilies.put(family, qualifiers);
        }
    }

    public void addQualifier(String family, String qualifier, CryptoTechnique.CryptoType cryptoType) {
        Map<String, CryptoTechnique.CryptoType> qualifierTemp = this.columnFamilies.get(family);
        qualifierTemp.put(qualifier, cryptoType);
        this.columnFamilies.put(family, qualifierTemp);
    }


    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Table Schema\n");
        sb.append("Table Name: ").append(this.tablename).append("\n");
        sb.append("Key CryptoType: ").append(this.key).append("\n");
        sb.append("Columns: \n");
        for(String family : this.columnFamilies.keySet()) {
            sb.append("> Family: ").append(family).append("\n");
            for(String qualifier : this.columnFamilies.get(family).keySet()) {
                sb.append("# Qualifier: ").append(qualifier).append("\n");
                sb.append("# CryptoType: ").append(this.columnFamilies.get(family).get(qualifier)).append("\n");
            }
        }

        return sb.toString();
    }



}

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
//    The CryptoTechnique to use in the keys
    private Key key;
//    The Column Families and the respective Column Qualifiers and CryptoTechniques
    private List<Family> columnFamilies;

    public TableSchema() {
        this.tablename = "";
        this.key = null;
        this.columnFamilies = new ArrayList<Family>();
    }

    public TableSchema(String tablename, Key key, List<Family> families) {
        this.tablename = tablename;
        this.key = key;
        this.columnFamilies = families;
    }

    public String getTablename() {
        return this.tablename;
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

    public void setKey(Key key) {
        this.key = key;
    }

    public void setColumnFamilies(List<Family> families) {
        this.columnFamilies = new ArrayList<Family>();

        for(Family family : families)
            this.columnFamilies.add(family);
    }

//    public void addFamily(String family) {
//        if (!this.columnFamilies.containsKey(family)) {
//            this.columnFamilies.put(family, new HashMap<String, CryptoTechnique.CryptoType>());
//        }
//    }
//
//    public void addFamily(String family, Map<String, CryptoTechnique.CryptoType> qualifiers) {
//        if (!this.columnFamilies.containsKey(family)) {
//            this.columnFamilies.put(family, qualifiers);
//        }
//    }
//
//    public void addQualifier(String family, String qualifier, CryptoTechnique.CryptoType cryptoType) {
//        Map<String, CryptoTechnique.CryptoType> qualifierTemp = this.columnFamilies.get(family);
//        qualifierTemp.put(qualifier, cryptoType);
//        this.columnFamilies.put(family, qualifierTemp);
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

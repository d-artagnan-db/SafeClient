package pt.uminho.haslab.safecloudclient.queryengine;

import org.apache.hadoop.hbase.HColumnDescriptor;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.schema.Family;
import pt.uminho.haslab.safecloudclient.schema.Key;
import pt.uminho.haslab.safecloudclient.schema.Qualifier;
import pt.uminho.haslab.safecloudclient.schema.TableSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by rgmacedo on 5/11/17.
 */
public class QEngineIntegration {
    private final int keyFormatSize = 10;
    private final int familyFormatSize = 32;
    private final CryptoTechnique.CryptoType cType = CryptoTechnique.CryptoType.OPE;

    public QEngineIntegration() {

    }

    public TableSchema buildQEngineTableSchema(String tablename, HColumnDescriptor[] descriptors) {
        Key key = new Key(cType, keyFormatSize);
        List<Family> columnFamilies = new ArrayList<>(descriptors.length);
        for(int i = 0; i < descriptors.length; i++) {
            Family temp_family = new Family(descriptors[i].getNameAsString(), cType, familyFormatSize);
            columnFamilies.add(temp_family);
        }

        return new TableSchema(
                tablename, //tablename
                cType,  //default key CryptoType
                cType,  //default columns CryptoType
                64, //default format size for both key and columns
                key, //key class (contains the secure properties to ensure the key privacy)
                columnFamilies); // list of column families (contains the secure properties to ensure the families and qualifiers privacy)

    }

    public boolean doesColumnFamilyExist(TableSchema ts, String family) {
        boolean status = false;
        for(Family f : ts.getColumnFamilies()) {
            if(f.getFamilyName().equals(family)) {
                status = true;
                break;
            }
        }

        return status;
    }


    public boolean doesFamilyContainsQualifier(TableSchema ts, String family, String qualifier) {
        boolean contains = false;
        for(Family f : ts.getColumnFamilies()) {
            if(f.getFamilyName().equals(family) && f.containsQualifier(qualifier)) {
                contains = true;
                break;
            }
        }
        return contains;
    }

    public Family createDefaultFamily(String familyName) {
        return new Family(familyName, this.cType, this.familyFormatSize);
    }

    public Qualifier createDefaultQualifier(String qualifierName, CryptoTechnique.CryptoType cType) {
        return new Qualifier(qualifierName, cType, this.familyFormatSize, new HashMap<String, String>());
    }

    public int getKeyFormatSize() {
        return this.keyFormatSize;
    }

    public int getFamilyFormatSize() {
        return this.familyFormatSize;
    }

    public CryptoTechnique.CryptoType getCryptographicTechnique() {
        return this.cType;
    }

}

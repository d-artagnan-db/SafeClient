//package pt.uminho.haslab.safecloudclient.queryengine;
//
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import pt.uminho.haslab.cryptoenv.CryptoTechnique;
//import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoTable;
//import pt.uminho.haslab.safecloudclient.schema.Family;
//import pt.uminho.haslab.safecloudclient.schema.Key;
//import pt.uminho.haslab.safecloudclient.schema.Qualifier;
//import pt.uminho.haslab.safecloudclient.schema.TableSchema;
//
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
///**
// * Created by rgmacedo on 5/11/17.
// */
//public class QEngineIntegration {
//    static final Log LOG = LogFactory.getLog(CryptoTable.class.getName());
//    private final int keyFormatSize = 100;
//    private final int familyFormatSize = 100;
//
////    WARNING: changed from OPE to PLT
//    private final CryptoTechnique.CryptoType cType = CryptoTechnique.CryptoType.PLT;
//
//
//    public QEngineIntegration() {
//
//    }
//
//    public TableSchema buildQEngineTableSchema(String tablename, HColumnDescriptor[] descriptors) {
//        Key key = new Key(cType, keyFormatSize);
//        List<Family> columnFamilies = new ArrayList<>(descriptors.length);
//
//        Family protected_family = isAProtectedTable(tablename);
//
////        Temporary Behavior
//        if(protected_family != null) {
//            LOG.debug("Protected family incoming.");
//            for (int i = 0; i < descriptors.length; i++) {
//                String descriptorName = descriptors[i].getNameAsString();
//                if(descriptorName.equals("DQE")) {
//                    LOG.debug("Descriptor equals to DQE. Protected family added to columnFamilies of table schema.");
//                    columnFamilies.add(protected_family);
//                }
//                else {
//                    Family temp_family = new Family(descriptors[i].getNameAsString(), cType, familyFormatSize);
//                    columnFamilies.add(temp_family);
//                }
//            }
//        } else {
////            Normal behavior
//            for (int i = 0; i < descriptors.length; i++) {
//                Family temp_family = new Family(descriptors[i].getNameAsString(), cType, familyFormatSize);
//                columnFamilies.add(temp_family);
//            }
//        }
//
//        return new TableSchema(
//                tablename, //tablename
//                cType,  //default key CryptoType
//                cType,  //default columns CryptoType
//                64, //default format size for both key and columns
//                key, //key class (contains the secure properties to ensure the key privacy)
//                columnFamilies); // list of column families (contains the secure properties to ensure the families and qualifiers privacy)
//
//    }
//
//    public boolean doesColumnFamilyExist(TableSchema ts, String family) {
//        boolean status = false;
//        for(Family f : ts.getColumnFamilies()) {
//            if(f.getFamilyName().equals(family)) {
//                status = true;
//                break;
//            }
//        }
//
//        return status;
//    }
//
//
//    public boolean doesFamilyContainsQualifier(TableSchema ts, String family, String qualifier) {
//        boolean contains = false;
//        for(Family f : ts.getColumnFamilies()) {
//            if(f.getFamilyName().equals(family) && f.containsQualifier(qualifier)) {
//                contains = true;
//                break;
//            }
//        }
//        return contains;
//    }
//
//    public Family createDefaultFamily(String familyName) {
//        return new Family(familyName, this.cType, this.familyFormatSize);
//    }
//
//    public Family createPersonalizedFamily(String familyName, CryptoTechnique.CryptoType cType, int formatSize) {
//        return new Family(familyName, cType, formatSize);
//    }
//
//    public Qualifier createDefaultQualifier(String qualifierName, CryptoTechnique.CryptoType cType) {
//        return new Qualifier(qualifierName, cType, this.familyFormatSize, new HashMap<String, String>());
//    }
//
//    public Qualifier createPersonalizedQualifier(String qualifierName, CryptoTechnique.CryptoType cType, int formatSize) {
//        return new Qualifier(qualifierName, cType, formatSize, new HashMap<String, String>());
//    }
//
//    public int getKeyFormatSize() {
//        return this.keyFormatSize;
//    }
//
//    public int getFamilyFormatSize() {
//        return this.familyFormatSize;
//    }
//
//    public CryptoTechnique.CryptoType getCryptographicTechnique() {
//        return this.cType;
//    }
//
//
//    public Family isAProtectedTable(String tableName) {
//        Family f = new Family("DQE", this.cType, this.familyFormatSize);
//        switch(tableName) {
//            case "R-maxdata-CLINIDATA_NEW-DTW_PATIENT":
////                R-maxdata-CLINIDATA_NEW-DTW_PATIENT:<family>:NAME
//                f.addQualifier(createPersonalizedQualifier("1", CryptoTechnique.CryptoType.DET, 100));
////                R-maxdata-CLINIDATA_NEW-DTW_PATIENT:<family>:BIRTHDAY_STAMP
//                f.addQualifier(createPersonalizedQualifier("2", CryptoTechnique.CryptoType.OPE, 16));
//                f.addQualifier(createPersonalizedQualifier("2_STD", CryptoTechnique.CryptoType.STD, 16));
////                  R-maxdata-CLINIDATA_NEW-DTW_PATIENT:<family>:CALCULATE_BIRTHDAY_STAMP
//                f.addQualifier(createPersonalizedQualifier("3", CryptoTechnique.CryptoType.OPE, 16));
//                f.addQualifier(createPersonalizedQualifier("3_STD", CryptoTechnique.CryptoType.STD, 16));
//
//                return f;
//            case "R-maxdata-CLINIDATA_NEW-DTW_PATIENT_ID_BY_PATIENT":
////                  R-maxdata-CLINIDATA_NEW-DTW_PATIENT_ID_BY_PATIENT:<family>:SUBJECT_ID
//                f.addQualifier(createPersonalizedQualifier("2", CryptoTechnique.CryptoType.DET, 5));
//
//                return f;
//            case "R-maxdata-CLINIDATA_NEW-DTW_TEST_RESULT":
////                R-maxdata-CLINIDATA_NEW-DTW_TEST_RESULT:<family>:NORMAL_VALUE
//                f.addQualifier(createPersonalizedQualifier("3", CryptoTechnique.CryptoType.STD, 4000));
//
//                return f;
//            default:
//                return null;
//        }
//    }
//
////    public Family isAProtectedTable(String tableName) {
////        Family f = new Family("DQE", this.cType, this.familyFormatSize);
////        switch(tableName) {
////            case "R-maxdata-CLINIDATA_NEW-DTW_PATIENT":
////                f.addQualifier(createPersonalizedQualifier("1", CryptoTechnique.CryptoType.PLT, 100));
////                f.addQualifier(createPersonalizedQualifier("2", CryptoTechnique.CryptoType.PLT, 16));
////                f.addQualifier(createPersonalizedQualifier("3", CryptoTechnique.CryptoType.PLT, 16));
////
////                return f;
////            case "R-maxdata-CLINIDATA_NEW-DTW_PATIENT_ID_BY_PATIENT":
////                f.addQualifier(createPersonalizedQualifier("2", CryptoTechnique.CryptoType.PLT, 5));
////
////                return f;
////            case "R-maxdata-CLINIDATA_NEW-DTW_TEST_RESULT":
////                f.addQualifier(createPersonalizedQualifier("3", CryptoTechnique.CryptoType.PLT, 4000));
////
////                return f;
////            default:
////                return null;
////        }
////    }
//
//}

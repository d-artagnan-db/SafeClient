package pt.uminho.haslab.safeclient.secureTable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import pt.uminho.haslab.OpeHgd;
import pt.uminho.haslab.cryptoenv.CryptoHandler;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safemapper.*;
import pt.uminho.haslab.safemapper.DatabaseSchema.CryptoType;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.*;

/**
 * CryptoProperties class.
 * Supports the CryptoTable class with the secondary methods such as encode, decode, ...
 */
public class CryptoProperties {
    static final Log LOG = LogFactory.getLog(CryptoTable.class.getName());

    //	TableSchema object, used to trace the database composition
    public TableSchema tableSchema;

    //	For each CryptoBox a new CryptoHandler object must be instantiated
    public CryptoHandler stdHandler;
    public CryptoHandler detHandler;
    //	This object handles the Row-Keys protected with the OPE CryptoBox
    public CryptoHandler opeHandler;
    //	Since the OPE CryptoBox need to be initialized with the plaintext size and ciphertext size, each value protected with OPE must have an individual CryptoHandler
    public Map<String, Map<String, CryptoHandler>> opeValueHandler;

    public CryptoHandler fpeHandler;
    public Map<String, Map<String, CryptoHandler>> fpeValueHandler;

    //	Cryptographic Keys stored in byte[] format
    public byte[] stdKey;
    public byte[] detKey;
    public byte[] opeKey;
    public Map<String, byte[]> fpeKey;

    public CryptoProperties(TableSchema ts) throws UnsupportedEncodingException {
        this.tableSchema = ts;

        this.stdHandler = new CryptoHandler(CryptoType.STD, new ArrayList<>());
        this.detHandler = new CryptoHandler(CryptoType.DET, new ArrayList<>());
        this.opeHandler = new CryptoHandler(CryptoType.OPE, opeArguments(
                this.tableSchema.getKey().getFormatSize(),
                this.tableSchema.getKey().getFormatSize() * 2));

        this.opeValueHandler = defineFamilyCryptoHandler(CryptoType.OPE);
//		this.verifyOpeValueHandler();

        if (this.tableSchema.getKey() instanceof KeyFPE) {
            KeyFPE temp_key_fpe = (KeyFPE) this.tableSchema.getKey();
            this.fpeHandler = new CryptoHandler(CryptoType.FPE, this.fpeArguments(temp_key_fpe));
            byte[] temp_key = temp_key_fpe.getSecurityParameters(this.fpeHandler.gen());
            this.fpeKey = new HashMap<>();
            this.fpeKey.put("KEY", temp_key);
        }
        this.fpeValueHandler = defineFamilyCryptoHandler(CryptoType.FPE);

    }

    public static CryptoTechnique.FFX whichFpeInstance(String instance) {
        switch (instance) {
            case "FF1":
                return CryptoTechnique.FFX.FF1;
            case "FF3":
                return CryptoTechnique.FFX.FF3;
            default:
                return null;
        }
    }

    public static byte[] getTweakBytes(String instance, String tweak) {
        byte[] temp = new byte[8];
        switch (instance) {
            case "FF1":
                temp = tweak.getBytes(Charset.forName("UTF-8"));
                break;
            case "FF3":
                byte[] temp_tweak = tweak.getBytes(Charset.forName("UTF-8"));
                if (temp_tweak.length == 8) {
                    temp = temp_tweak;
                } else if (temp_tweak.length > 8) {
                    System.arraycopy(temp_tweak, 0, temp, 0, 8);
                } else {
                    throw new IllegalArgumentException("For an FF3 instance, the tweak must have a 64bit length");
                }
                break;
        }
        return temp;
    }

    /**
     * opeArguments(formatSize : int, ciphertextSize : int) method :  set the ope initialization values.
     * By default the OPE Cache is enabled (OpeHgd.CACHE). To disable the OPE CACHE set OpeHdg.NO_CACHE
     *
     * @param formatSize     plaintext format size
     * @param ciphertextSize ciphertext format size
     * @return the ope arguments list (List<Object>)
     */
    public List<Object> opeArguments(int formatSize, int ciphertextSize) {
        List<Object> arguments = new ArrayList<>();
        arguments.add(OpeHgd.CACHE);
        arguments.add(formatSize);
        arguments.add(ciphertextSize);
        return arguments;
    }

    public List<Object> fpeArguments(Object fpe_object) {
        List<Object> fpeArguments = new ArrayList<>();

        if (fpe_object instanceof KeyFPE) {
            KeyFPE temp = (KeyFPE) fpe_object;
            fpeArguments.add(temp.getInstance());
            fpeArguments.add(temp.getRadix());
        } else if (fpe_object instanceof QualifierFPE) {
            QualifierFPE temp = (QualifierFPE) fpe_object;
            fpeArguments.add(temp.getInstance());
            fpeArguments.add(temp.getRadix());
        }
        return fpeArguments;
    }

    /**
     * defineFamilyCryptoHandler() method : creates a Map of CryptoHandler's for each qualifier protected with OPE CryptoBox.
     *
     * @return a collection of families and the respective qualifiers CryptoHandler (Map<FamilyName, Map<QualifierName, CryptoHandler>>)
     */
    public Map<String, Map<String, CryptoHandler>> defineFamilyCryptoHandler(CryptoType cType) {
        Map<String, Map<String, CryptoHandler>> familyCryptoHandler = new HashMap<>();
        for (Family f : this.tableSchema.getColumnFamilies()) {
            Map<String, CryptoHandler> qualifierCryptoHandler = new HashMap<>();
            for (Qualifier q : f.getQualifiers()) {
                if (q.getCryptoType().equals(cType)) {
                    CryptoHandler ch;
                    switch (cType) {
                        case OPE:
                            ch = new CryptoHandler(cType, opeArguments(q.getFormatSize(), q.getFormatSize() * 2));
                            break;
                        case FPE:
                            QualifierFPE qFPE = (QualifierFPE) q;
                            ch = new CryptoHandler(cType, fpeArguments(qFPE));

                            break;
                        default:
                            ch = null;
                    }
                    qualifierCryptoHandler.put(q.getName(), ch);
                }
            }
            familyCryptoHandler.put(f.getFamilyName(), qualifierCryptoHandler);
        }
        return familyCryptoHandler;
    }

    /**
     * getCryptoHandler(family : String, qualifier : String) method : get an OPE CryptoHandler of a given family and qualifier
     *
     * @param family
     * @param qualifier
     * @return the CryptoHandler that corresponds to the family and qualifier specified
     */
    public CryptoHandler getCryptoHandler(CryptoType ctype, String family, String qualifier) {
        switch (ctype) {
            case OPE:
//				Check if Qualifier CryptoHandler is available. If not, return the CryptoHandler of the respective family
                if (this.opeValueHandler.containsKey(family) && this.opeValueHandler.get(family).containsKey(qualifier)) {
                    return this.opeValueHandler.get(family).get(qualifier);
                } else {
                    Family temp_family = this.tableSchema.getFamily(family);
                    List<Object> temp_args = new ArrayList<>();
                    temp_args.add(temp_family.getFormatSize());
                    temp_args.add(temp_family.getFormatSize() * 2);

                    return new CryptoHandler(temp_family.getCryptoType(), temp_args);
                }
            case FPE:
                return this.fpeValueHandler.get(family).get(qualifier);
            default:
                return null;
        }
    }

    /**
     * setKey (cType : CryptoType, key : byte[]) : set the cryptographic key of a given CryptoBox type
     *
     * @param cType CryptoBox type
     * @param key   Cryptographic key in byte[] format
     */
    public void setKey(DatabaseSchema.CryptoType cType, byte[] key) throws UnsupportedEncodingException {
        switch (cType) {
            case STD:
                this.stdKey = key;
                break;
            case DET:
                this.detKey = key;
                break;
            case OPE:
                this.opeKey = key;
                break;
            case FPE:
                KeyFPE temp_key_fpe = (KeyFPE) this.tableSchema.getKey();
                this.fpeKey.put("KEY", temp_key_fpe.getSecurityParameters(key));
                setQualifiersFPEKey(key);
                break;
            default:
                break;
        }
    }

    public void setQualifiersFPEKey(byte[] key) throws UnsupportedEncodingException {
        for (Family f : this.tableSchema.getColumnFamilies()) {
            for (Qualifier q : f.getQualifiers()) {
                if (q.getCryptoType().equals(CryptoType.FPE)) {
                    QualifierFPE qFPE = (QualifierFPE) q;
                    this.fpeKey.put(f.getFamilyName() + ":" + qFPE.getName(), qFPE.getSecurityParameters(key));
                }
            }
        }
    }

    /**
     * encodeRowCryptoType(cType : CryptoType, content : byte[]) method : encrypts the Row-Key
     *
     * @param cType   CryptoBox type
     * @param content plaintext Row-Key
     * @return the resulting ciphertext
     */
    private byte[] encodeRowCryptoType(CryptoType cType, byte[] content) {
        byte[] row;
        if (this.tableSchema.getKeyPadding()) {
            row = Utils.addPadding(content, this.tableSchema.getKeyFormatSize());
        } else {
            row = content;
        }

        switch (cType) {
            case SMPC:
            case ISMPC:
            case LSMPC:
            case PLT:
                return row;
            case STD:
                return this.stdHandler.encrypt(this.stdKey, row);
            case DET:
                return this.detHandler.encrypt(this.detKey, row);
            case OPE:
                return this.opeHandler.encrypt(this.opeKey, row);
            case FPE:
                return this.fpeHandler.encrypt(this.fpeKey.get("KEY"), row);
            default:
                return null;
        }
    }

    /**
     * encodeValueCryptoType(cType : CryptoType, content : byte[], family : String, qualifier : String) method : encrypts the value of a specific family and qualifier
     *
     * @param cType     CryptoBox type
     * @param content   plaintext value
     * @param family    family column
     * @param qualifier qualifier column
     * @return the resulting ciphertext
     */
    private byte[] encodeValueCryptoType(CryptoType cType, byte[] content, String family, String qualifier) {
        byte[] value;
        if (this.tableSchema.getColumnPadding(family, qualifier)) {
            value = Utils.addPadding(content, this.tableSchema.getFormatSizeFromQualifier(family, qualifier));
        } else {
            value = content;
        }
        switch (cType) {
            case SMPC:
            case ISMPC:
            case LSMPC:
            case XOR:
            case PLT:
                return value;
            case STD:
                return this.stdHandler.encrypt(this.stdKey, value);
            case DET:
                return this.detHandler.encrypt(this.detKey, value);
            case OPE:
                CryptoHandler opeCh = getCryptoHandler(CryptoType.OPE, family, qualifier);
                return opeCh.encrypt(this.opeKey, value);
            case FPE:
                CryptoHandler fpeCH = getCryptoHandler(CryptoType.FPE, family, qualifier);
                return fpeCH.encrypt(this.fpeKey.get(family + ":" + qualifier), value);
            default:
                throw new NullPointerException("Cryptographic Type not recognized.");
        }
    }

    /**
     * decodeRowCryptoType(cType : CryptoType, ciphertext : byte[]) method : decrypts the Row-Key ciphertext
     *
     * @param cType      CryptoBox type
     * @param ciphertext protected Row-Key
     * @return the original Row-Key in byte[] format
     */
    private byte[] decodeRowCryptoType(CryptoType cType, byte[] ciphertext) {
        byte[] row;
        switch (cType) {
            case SMPC:
            case ISMPC:
            case LSMPC:
            case PLT:
                row = ciphertext;
                break;
            case STD:
                row = this.stdHandler.decrypt(this.stdKey, ciphertext);
                break;
            case DET:
                row = this.detHandler.decrypt(this.detKey, ciphertext);
                break;
            case OPE:
                row = this.opeHandler.decrypt(this.opeKey, ciphertext);
                break;
            case FPE:
                row = this.fpeHandler.decrypt(this.fpeKey.get("KEY"), ciphertext);
                break;
            default:
                throw new NullPointerException("Cryptographic Type not recognized.");
        }

        if (this.tableSchema.getKeyPadding()) {
            return Utils.removePadding(row);
        } else {
            return ciphertext;
        }

    }

    /**
     * decodeValueCryptoType(cType : CryptoType, ciphertext : byte[], family : String, qualifier : String) method : decrypts the ciphertext value of a specific family and qualifier
     *
     * @param cType      CryptoBox type
     * @param ciphertext protected value
     * @param family     family column
     * @param qualifier  qualifier column
     * @return the original value in byte[] format
     */
    private byte[] decodeValueCryptoType(CryptoType cType, byte[] ciphertext, String family, String qualifier) {
        byte[] value;
        switch (cType) {
            case SMPC:
            case ISMPC:
            case LSMPC:
            case XOR:
            case PLT:
                value = ciphertext;
                break;
            case STD:
                value = this.stdHandler.decrypt(this.stdKey, ciphertext);
                break;
            case DET:
                value = this.detHandler.decrypt(this.detKey, ciphertext);
                break;
            case OPE:
                CryptoHandler opeCh = getCryptoHandler(CryptoType.OPE, family, qualifier);
                value = opeCh.decrypt(this.opeKey, ciphertext);
                break;
            case FPE:
                CryptoHandler fpeCH = getCryptoHandler(CryptoType.FPE, family, qualifier);
                value = fpeCH.decrypt(this.fpeKey.get(family + ":" + qualifier), ciphertext);
                break;
            default:
                throw new NullPointerException("Cryptographic Type not recognized.");
        }

        if (this.tableSchema.getColumnPadding(family, qualifier)) {
            return Utils.removePadding(value);
        } else {
            return value;
        }

    }

    /**
     * encodeRow(content : byte[]) method : get the Row-Key CryptoType and encrypt the Row-Key
     *
     * @param content plaintext Row-Key
     * @return call the encodeRowCryptoType method. Return the encoded Row-Key in byte[] format
     */
    public byte[] encodeRow(byte[] content) {
        CryptoType cryptoType = this.tableSchema.getKey().getCryptoType();
        return encodeRowCryptoType(cryptoType, content);
    }

    /**
     * decodeRow(content : byte[]) method : get the Row-Key CryptoType and decrypt the Row-Key
     *
     * @param content ciphertext Row-Key
     * @return call the decodeRowCryptoType method. Return the original plaintext Row-Key in byte[] format
     */
    public byte[] decodeRow(byte[] content) {
        CryptoType cryptoType = this.tableSchema.getKey().getCryptoType();
        return decodeRowCryptoType(cryptoType, content);
    }

    /**
     * encodeValue(family : byte[], qualifier : byte[], value : byte[]) method : get the family:qualifier CryptoType and encrypt the value
     *
     * @param family    family column
     * @param qualifier qualifier column
     * @param value     plaintext value
     * @return call the encodeValueCryptoType method. Return the encoded value in byte[] format
     */
    public byte[] encodeValue(byte[] family, byte[] qualifier, byte[] value) {
        String f = new String(family, Charset.forName("UTF-8"));
        String q = new String(qualifier, Charset.forName("UTF-8"));
        CryptoType cryptoType = this.tableSchema.getCryptoTypeFromQualifier(f, q);
        return encodeValueCryptoType(cryptoType, value, f, q);
    }

    /**
     * decodeValue(family : byte[], qualifier : byte[], value : byte[]) method : get the family:qualifier CryptoType and decrypt the value
     *
     * @param family    family column
     * @param qualifier qualifier column
     * @param value     ciphertext value
     * @return call the decodeValueCryptoType method. Return the original value in byte[] format
     */
    public byte[] decodeValue(byte[] family, byte[] qualifier, byte[] value) {
        String f = new String(family, Charset.forName("UTF-8"));
        String q = new String(qualifier, Charset.forName("UTF-8"));

        CryptoType cryptoType = this.tableSchema.getCryptoTypeFromQualifier(f, q);
//		If CryptoType equals to null, it means that either an error occurred in the Qualifier creation or the Qualifier
// 		instance does not exists.
        if (cryptoType == null) {
            cryptoType = this.tableSchema.getFamily(new String(family)).getCryptoType();
        }

        return decodeValueCryptoType(cryptoType, value, f, q);
    }

    /**
     * decodeResult(row :byte[], res : Result) method : decrypt all qualifiers from the HBase Result encrypted object
     *
     * @param row original Row-Key
     * @param res HBase Result object encrypted
     * @return decrypted HBase Result
     */
    public Result decodeResult(byte[] row, Result res) {
        String opeValues = "_STD";
        List<Cell> cellList = new ArrayList<>();

        while (res.advance()) {
            Cell cell = res.current();
            byte[] cf = CellUtil.cloneFamily(cell);
            byte[] cq = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);
            long timestamp = cell.getTimestamp();
            byte type = cell.getTypeByte();

            Cell decCell;
            String qualifier = new String(cq, Charset.forName("UTF-8"));
            CryptoType cellCryptoType = this.tableSchema.getCryptoTypeFromQualifier(new String(cf, Charset.forName("UTF-8")), qualifier);

//			Verify if the actual qualifier is equal to <qualifier>_STD
            boolean verifyProperty = false;
            if (cellCryptoType == CryptoType.STD) {
                if (qualifier.length() >= opeValues.length()) {
                    verifyProperty = qualifier.substring(qualifier.length() - opeValues.length(), qualifier.length()).equals(opeValues);
                }
            }

            if (!verifyProperty && cf.length > 0) {
//				If the qualifier's CryptoType equal to OPE, decrypt the auxiliary qualifier (<qualifier_STD>)
                if (cellCryptoType == CryptoType.OPE) {
                    Cell stdCell = res.getColumnLatestCell(cf, (qualifier + opeValues).getBytes(Charset.forName("UTF-8")));
                    decCell = CellUtil.createCell(
                            row,
                            cf,
                            cq,
                            stdCell.getTimestamp(),
                            stdCell.getTypeByte(),
                            this.decodeValue(cf, CellUtil.cloneQualifier(stdCell), CellUtil.cloneValue(stdCell))

                    );

                } else {
                    decCell = CellUtil.createCell(row, cf, cq, timestamp, type, this.decodeValue(cf, cq, value));
                }

                cellList.add(decCell);

            } else if (cf.length == 0) {
                decCell = CellUtil.createCell(row, cf, cq, timestamp, type, value);
                cellList.add(decCell);
            }
        }
        return Result.create(cellList);
    }

    /**
     * getHColumnDescriptors(familiesAndQualifiers : Map<byte[], NavigableSet<byte[]>>) method : convert a mapper of the
     * column families and the respective column qualifiers in a user friendly one
     *
     * @param familiesAndQualifiers received mapper
     * @return user friendly mapper, providing the column families and the respective column qualifiers in the Map<byte[], List<byte[]>> format.
     */
    public Map<byte[], List<byte[]>> getHColumnDescriptors(Map<byte[], NavigableSet<byte[]>> familiesAndQualifiers) {
        String opeValue = "_STD";
        Map<byte[], List<byte[]>> result = new HashMap<>();
        for (byte[] family : familiesAndQualifiers.keySet()) {
            NavigableSet<byte[]> q = familiesAndQualifiers.get(family);
            if (q == null) {
//				LOG.info("CryptoProperties:getHColumnDescriptors:"+ familiesAndQualifiers.toString());
                if (this.tableSchema.containsFamily(new String(family))) {
                    List<byte[]> q_list = new ArrayList<>();
                    for (Qualifier q_temp : this.tableSchema.getFamily(new String(family)).getQualifiers()) {
                        q_list.add(q_temp.getName().getBytes());
                    }

                    result.put(family, q_list);

                } else {
                    result.put(family, null);
                }

            } else if (!q.isEmpty()) {
                Iterator i = q.iterator();
                List<byte[]> qualifierList = new ArrayList<>();
                while (i.hasNext()) {
                    byte[] qualifier = (byte[]) i.next();
                    qualifierList.add(qualifier);
                    if (tableSchema.getCryptoTypeFromQualifier(new String(family, Charset.forName("UTF-8")), new String(qualifier, Charset.forName("UTF-8"))) == CryptoType.OPE) {
                        String q_std = new String(qualifier, Charset.forName("UTF-8"));
                        qualifierList.add((q_std + opeValue).getBytes(Charset.forName("UTF-8")));
                    }

                }
                result.put(family, qualifierList);
            }
        }

        return result;
    }

    //	TODO support more Comparators
    public ByteArrayComparable checkComparatorType(ByteArrayComparable comparable, byte[] encoded_content, CryptoType cType) {
        String comparator_name = comparable.getClass().getSimpleName();

        if (!comparator_name.isEmpty()) {
            switch (comparator_name) {
                case "BinaryPrefixComparator":
                    if (cType == CryptoType.PLT) {
                        return new BinaryPrefixComparator(encoded_content);
                    } else {
                        LOG.error("UnsupportedOperationException: BinaryPrefixComparator not supported for the current CryptoBoxes.");
                        throw new UnsupportedOperationException("BinaryPrefixComparator not supported for the current CryptoBoxes.");
                    }
                case "BinaryComparator":
                    return new BinaryComparator(encoded_content);
                default:
                    LOG.error(comparator_name + " not supported for the current CyrptoBoxes.");
                    throw new UnsupportedOperationException(comparator_name + " not supported for the current CryptoBoxes.");
            }
        } else {
            LOG.error("NullPointerException: ByteArrayComparable object cannot be empty or null.");
            throw new NullPointerException("ByteArrayComparable object cannot be empty or null.");
        }
    }

}

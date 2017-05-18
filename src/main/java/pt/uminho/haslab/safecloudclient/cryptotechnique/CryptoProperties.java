package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.OpeHgd;
import pt.uminho.haslab.cryptoenv.CryptoHandler;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.schema.*;

import java.nio.charset.Charset;
import java.util.*;

/**
 * CryptoProperties class.
 * Supports the CryptoTable class with the secondary methods such as encode, decode, ...
 */
public class CryptoProperties {

//	TableSchema object, used to trace the database composition
	public TableSchema tableSchema;

//	For each CryptoBox a new CryptoHandler object must be instantiated
	public CryptoHandler stdHandler;
	public CryptoHandler detHandler;
//	This object handles the row keys protected with the OPE CryptoBox
	public CryptoHandler opeHandler;
//	Since the OPE CryptoBox need to be initialized with the plaintext size and ciphertext size, each value protected with OPE must have an individual CryptoHandler
	public Map<String, Map<String, CryptoHandler>> opeValueHandler;

	public CryptoHandler fpeHandler;
	public Map<String, Map<String, CryptoHandler>> fpeValueHandler;

//	Cryptographic Keys stored in byte[] format
	public byte[] stdKey;
	public byte[] detKey;
	public byte[] opeKey;
	public Map<String, byte[]>fpeKey;

	public CryptoProperties(TableSchema ts) {
		this.tableSchema = ts;

		this.stdHandler = new CryptoHandler(CryptoTechnique.CryptoType.STD, new ArrayList<>());
		this.stdKey = this.stdHandler.gen();

		this.detHandler = new CryptoHandler(CryptoTechnique.CryptoType.DET, new ArrayList<>());
		this.detKey = this.detHandler.gen();

		this.opeHandler = new CryptoHandler(CryptoTechnique.CryptoType.OPE, opeArguments(
				this.tableSchema.getKey().getFormatSize(),
				this.tableSchema.getKey().getFormatSize()*2));
		this.opeKey = this.opeHandler.gen();

		this.opeValueHandler = defineFamilyCryptoHandler(CryptoTechnique.CryptoType.OPE);
//		this.verifyOpeValueHandler();

		if(this.tableSchema.getKey() instanceof KeyFPE) {
			KeyFPE temp_key_fpe = (KeyFPE) this.tableSchema.getKey();
			this.fpeHandler = new CryptoHandler(CryptoTechnique.CryptoType.FPE, this.fpeArguments(temp_key_fpe));
			byte[] temp_key = temp_key_fpe.getSecurityParameters(this.fpeHandler.gen());
			this.fpeKey = new HashMap<>();
			this.fpeKey.put("KEY",temp_key);
		}
		this.fpeValueHandler = defineFamilyCryptoHandler(CryptoTechnique.CryptoType.FPE);

	}

	/**
	 * opeArguments(formatSize : int, ciphertextSize : int) method :  set the ope initialization values.
	 * By default the OPE Cache is enabled (OpeHgd.CACHE). To disable the OPE CACHE set OpeHdg.NO_CACHE
	 * @param formatSize plaintext format size
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
	 * @return a collection of families and the respective qualifiers CryptoHandler (Map<FamilyName, Map<QualifierName, CryptoHandler>>)
	 */
	public Map<String, Map<String, CryptoHandler>> defineFamilyCryptoHandler(CryptoTechnique.CryptoType cType) {
		Map<String, Map<String, CryptoHandler>> familyCryptoHandler = new HashMap<>();
		for(Family f : this.tableSchema.getColumnFamilies()) {
			Map<String, CryptoHandler> qualifierCryptoHandler = new HashMap<>();
			for(Qualifier q : f.getQualifiers()) {
				if(q.getCryptoType().equals(cType)) {
					CryptoHandler ch;
					switch (cType) {
						case OPE :
							ch = new CryptoHandler(cType, opeArguments(q.getFormatSize(), q.getFormatSize()*2));
							break;
						case FPE :
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
	 * verifyOpeValueHandler() method : only used to check the OPE CryptoHandlers
	 */
	public void verifyOpeValueHandler() {
		for(String family : this.opeValueHandler.keySet()) {
			System.out.println("Family Size: "+this.opeValueHandler.get(family).size());
			for(String qualifier : this.opeValueHandler.get(family).keySet()) {
				System.out.println("Qualifier Properties: "+qualifier+" - "+this.opeValueHandler.get(family).get(qualifier).toString());
			}
		}
	}

	/**
	 * getCryptoHandler(family : String, qualifier : String) method : get an OPE CryptoHandler of a given family and qualifier
	 * @param family
	 * @param qualifier
	 * @return the CryptoHandler that corresponds to the family and qualifier specified
	 */
	public CryptoHandler getCryptoHandler(CryptoTechnique.CryptoType ctype, String family, String qualifier) {
		switch(ctype) {
			case OPE :
				return this.opeValueHandler.get(family).get(qualifier);
			case FPE :
				return this.fpeValueHandler.get(family).get(qualifier);
			default :
				return null;
		}
	}

	/**
	 * getKey(cType : CryptoType) method : get cryptographic key given a CryptoBox type
	 * @param cType CryptoBox type
	 * @return the respective cryptographic key
	 */
	public byte[] getKey(CryptoTechnique.CryptoType cType) {
		switch (cType) {
			case STD :
				return stdKey;
			case DET :
				return detKey;
			case OPE :
				return opeKey;
			case FPE :
				return fpeKey.get("KEY");
			default :
				return null;
		}
	}

	/**
	 * setKey (cType : CryptoType, key : byte[]) : set the cryptographic key of a given CryptoBox type
	 * @param cType CryptoBox type
	 * @param key Cryptographic key in byte[] format
	 */
	public void setKey(CryptoTechnique.CryptoType cType, byte[] key) {
		switch (cType) {
			case STD :
				this.stdKey = key;
				break;
			case DET :
				this.detKey = key;
				break;
			case OPE :
				this.opeKey = key;
				break;
			case FPE :
				KeyFPE temp_key_fpe = (KeyFPE) this.tableSchema.getKey();
				this.fpeKey.put("KEY",temp_key_fpe.getSecurityParameters(key));
				setQualifiersFPEKey(key);
				break;
			default :
				break;
		}
//		System.out.println("The key was setted. Key - " + Arrays.toString(key));
	}

	public void setQualifiersFPEKey(byte[] key) {
		for (Family f : this.tableSchema.getColumnFamilies()) {
			for (Qualifier q : f.getQualifiers()) {
				if (q.getCryptoType().equals(CryptoTechnique.CryptoType.FPE)) {
					QualifierFPE qFPE = (QualifierFPE) q;
					this.fpeKey.put(f.getFamilyName() + ":" + qFPE.getName(), qFPE.getSecurityParameters(key));
				}
			}
		}
	}

	/**
	 * encodeRowCryptoType(cType : CryptoType, content : byte[]) method : encrypts the row key
	 * @param cType CryptoBox type
	 * @param content plaintext row key
	 * @return the resulting ciphertext
	 */
	private byte[] encodeRowCryptoType(CryptoTechnique.CryptoType cType, byte[] content) {
		switch (cType) {
			case PLT :
				return content;
			case STD :
				return this.stdHandler.encrypt(this.stdKey, content);
			case DET :
				return this.detHandler.encrypt(this.detKey, content);
			case OPE :
				return this.opeHandler.encrypt(this.opeKey, content);
			case FPE :
				return this.fpeHandler.encrypt(this.fpeKey.get("KEY"), content);
			default :
				return null;
		}
	}

	/**
	 * encodeValueCryptoType(cType : CryptoType, content : byte[], family : String, qualifier : String) method : encrypts the value of a specific family and qualifier
	 * @param cType CryptoBox type
	 * @param content plaintext value
	 * @param family family column
	 * @param qualifier qualifier column
	 * @return the resulting ciphertext
	 */
	private byte[] encodeValueCryptoType(CryptoTechnique.CryptoType cType, byte[] content, String family, String qualifier) {
		switch (cType) {
			case PLT :
				return content;
			case STD :
				return this.stdHandler.encrypt(this.stdKey, content);
			case DET :
				return this.detHandler.encrypt(this.detKey, content);
			case OPE :
				CryptoHandler opeCh = getCryptoHandler(CryptoTechnique.CryptoType.OPE, family, qualifier);
				return opeCh.encrypt(this.opeKey, content);
			case FPE :
				CryptoHandler fpeCH = getCryptoHandler(CryptoTechnique.CryptoType.FPE, family, qualifier);
				return fpeCH.encrypt(this.fpeKey.get(family+":"+qualifier), content);
			default :
				return null;
		}
	}

	/**
	 * decodeRowCryptoType(cType : CryptoType, ciphertext : byte[]) method : decrypts the row key ciphertext
	 * @param cType CryptoBox type
	 * @param ciphertext protected row key
	 * @return the original row key in byte[] format
	 */
	private byte[] decodeRowCryptoType(CryptoTechnique.CryptoType cType, byte[] ciphertext) {
		switch (cType) {
			case PLT :
				return ciphertext;
			case STD :
				return this.stdHandler.decrypt(this.stdKey, ciphertext);
			case DET :
				return this.detHandler.decrypt(this.detKey, ciphertext);
			case OPE :
				return this.opeHandler.decrypt(this.opeKey, ciphertext);
			case FPE :
				return this.fpeHandler.decrypt(this.fpeKey.get("KEY"), ciphertext);
			default :
				return null;
		}
	}

	/**
	 * decodeValueCryptoType(cType : CryptoType, ciphertext : byte[], family : String, qualifier : String) method : decrypts the ciphertext value of a specific family and qualifier
	 * @param cType CryptoBox type
	 * @param ciphertext protected value
	 * @param family family column
	 * @param qualifier qualifier column
	 * @return the original value in byte[] format
	 */
	private byte[] decodeValueCryptoType(CryptoTechnique.CryptoType cType, byte[] ciphertext, String family, String qualifier) {
		switch (cType) {
			case PLT :
				return ciphertext;
			case STD :
				return this.stdHandler.decrypt(this.stdKey, ciphertext);
			case DET :
				return this.detHandler.decrypt(this.detKey, ciphertext);
			case OPE :
				CryptoHandler opeCh = getCryptoHandler(CryptoTechnique.CryptoType.OPE, family, qualifier);
				return opeCh.decrypt(this.opeKey, ciphertext);
			case FPE :
				CryptoHandler fpeCH = getCryptoHandler(CryptoTechnique.CryptoType.FPE, family, qualifier);
				return fpeCH.decrypt(this.fpeKey.get(family+":"+qualifier), ciphertext);
			default :
				return null;
		}
	}

	/**
	 * encodeRow(content : byte[]) method : get the row key CryptoType and encrypt the row key
	 * @param content plaintext row key
	 * @return call the encodeRowCryptoType method. Return the encoded row key in byte[] format
	 */
	public byte[] encodeRow(byte[] content) {
		CryptoTechnique.CryptoType cryptoType = this.tableSchema.getKey().getCryptoType();
//		System.out.println("Encode Row: " + cryptoType);
		return encodeRowCryptoType(cryptoType, content);
	}

	/**
	 * decodeRow(content : byte[]) method : get the row key CryptoType and decrypt the row key
	 * @param content ciphertext row key
	 * @return call the decodeRowCryptoType method. Return the original plaintext row key in byte[] format
	 */
	public byte[] decodeRow(byte[] content) {
		CryptoTechnique.CryptoType cryptoType = this.tableSchema.getKey().getCryptoType();
//		System.out.println("Decode Row: " + cryptoType);
		return decodeRowCryptoType(cryptoType, content);
	}

	/**
	 * encodeValue(family : byte[], qualifier : byte[], value : byte[]) method : get the family:qualifier CryptoType and encrypt the value
	 * @param family family column
	 * @param qualifier qualifier column
	 * @param value plaintext value
	 * @return call the encodeValueCryptoType method. Return the encoded value in byte[] format
	 */
	public byte[] encodeValue(byte[] family, byte[] qualifier, byte[] value) {
		String f = new String(family, Charset.forName("UTF-8"));
		String q = new String(qualifier, Charset.forName("UTF-8"));
		CryptoTechnique.CryptoType cryptoType = this.tableSchema.getCryptoTypeFromQualifier(f, q);
//		System.out.println("Encode Value (" + f + "," + q + "): " + cryptoType);
		return encodeValueCryptoType(cryptoType, value, f, q);
	}

	/**
	 * decodeValue(family : byte[], qualifier : byte[], value : byte[]) method : get the family:qualifier CryptoType and decrypt the value
	 * @param family family column
	 * @param qualifier qualifier column
	 * @param value ciphertext value
	 * @return call the decodeValueCryptoType method. Return the original value in byte[] format
	 */
	public byte[] decodeValue(byte[] family, byte[] qualifier, byte[] value) {
		String f = new String(family, Charset.forName("UTF-8"));
		String q = new String(qualifier, Charset.forName("UTF-8"));
		CryptoTechnique.CryptoType cryptoType = this.tableSchema.getCryptoTypeFromQualifier(f, q);
//		System.out.println("Decode Value (" + f + "," + q + "): " + cryptoType);
		return decodeValueCryptoType(cryptoType, value, f, q);
	}

	/**
	 * decodeResult(row :byte[], res : Result) method : decrypt all qualifiers from the HBase Result encrypted object
	 * @param row original row key
	 * @param res HBase Result object encrypted
	 * @return decrypted HBase Result
	 */
	public Result decodeResult(byte[] row, Result res) {
//		byte[] decodedRow = this.decodeRow(row);
		String opeValues = "_STD";
		List<Cell> cellList = new ArrayList<Cell>();

		while (res.advance()) {
			Cell cell = res.current();
			byte[] cf = CellUtil.cloneFamily(cell);
			byte[] cq = CellUtil.cloneQualifier(cell);
			byte[] value = CellUtil.cloneValue(cell);
			long timestamp = cell.getTimestamp();
			byte type = cell.getTypeByte();

			Cell decCell;
			String qualifier = new String(cq, Charset.forName("UTF-8"));

//			Verify if the actual qualifier is equal to <qualifier>_STD
			boolean verifyProperty = false;
			if(qualifier.length() >= opeValues.length()) {
				verifyProperty = qualifier.substring(qualifier.length()-opeValues.length(), qualifier.length()).equals(opeValues);
			}

			if(!verifyProperty) {
//				If the qualifier's CryptoType equal to OPE, decrypt the auxiliary qualifier (<qualifier_STD>)
				if (tableSchema.getCryptoTypeFromQualifier(new String(cf, Charset.forName("UTF-8")), qualifier) == CryptoTechnique.CryptoType.OPE) {
					Cell stdCell = res.getColumnLatestCell(cf, (qualifier + opeValues).getBytes(Charset.forName("UTF-8")));

					decCell = CellUtil.createCell(
							row,
							cf,
							cq,
							stdCell.getTimestamp(),
							stdCell.getTypeByte(),
							this.decodeValue(
									cf,
									CellUtil.cloneQualifier(stdCell),
									CellUtil.cloneValue(stdCell))

					);
				} else {
					decCell = CellUtil.createCell(
							row,
							cf,
							cq,
							timestamp,
							type,
							this.decodeValue(cf, cq, value));
				}
				cellList.add(decCell);
			}
		}
		return Result.create(cellList);
	}

	/**
	 * getFamiliesAndQualifiers(familiesAndQualifiers : Map<byte[], NavigableSet<byte[]>>) method : convert a mapper of the
	 * column families and the respective column qualifiers in a user friendly one
	 * @param familiesAndQualifiers received mapper
	 * @return user friendly mapper, providing the column families and the respective column qualifiers in the Map<byte[], List<byte[]>> format.
	 */
	public Map<byte[], List<byte[]>> getFamiliesAndQualifiers(Map<byte[], NavigableSet<byte[]>> familiesAndQualifiers) {
		String opeValue = "_STD";
		Map<byte[],List<byte[]>> result = new HashMap<>();
		for(byte[] family : familiesAndQualifiers.keySet()) {
			NavigableSet<byte[]> q = familiesAndQualifiers.get(family);
			if (!q.isEmpty()) {
				Iterator i = q.iterator();
				List<byte[]> qualifierList = new ArrayList<>();
				while (i.hasNext()) {
					byte[] qualifier = (byte[]) i.next();
					qualifierList.add(qualifier);
					if (tableSchema.getCryptoTypeFromQualifier(new String(family, Charset.forName("UTF-8")), new String(qualifier, Charset.forName("UTF-8"))) == CryptoTechnique.CryptoType.OPE) {
						String q_std = new String(qualifier, Charset.forName("UTF-8"));
						qualifierList.add((q_std + opeValue).getBytes(Charset.forName("UTF-8")));
					}

				}
				result.put(family, qualifierList);
			}
		}

		return result;
	}


	public static CryptoTechnique.FFX whichFpeInstance(String instance) {
		switch (instance) {
			case "FF1" :
				return CryptoTechnique.FFX.FF1;
			case "FF3" :
				return CryptoTechnique.FFX.FF3;
			default :
				return null;
		}
	}


	public static byte[] getTweakBytes(String instance, String tweak) {
		byte[] temp = new byte[8];
		switch (instance) {
			case "FF1" :
				temp = tweak.getBytes(Charset.forName("UTF-8"));
				break;
			case "FF3" :
				byte[] temp_tweak = tweak.getBytes(Charset.forName("UTF-8"));
				if(temp_tweak.length == 8) {
					temp = temp_tweak;
				}
				else if(temp_tweak.length > 8) {
					System.arraycopy(temp_tweak, 0, temp, 0, 8);
				}
				else {
					throw new IllegalArgumentException("For an FF3 instance, the tweak must have a 64bit length");
				}
				break;
		}
		return temp;
	}

}

package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.OpeHgd;
import pt.uminho.haslab.cryptoenv.CryptoHandler;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.queryengine.QEngineIntegration;
import pt.uminho.haslab.safecloudclient.schema.*;

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

	public void replaceQualifierCryptoHandler(String family, String qualifier, CryptoTechnique.CryptoType cType, int formatSize) {
		switch(cType) {
			case OPE:
				CryptoHandler cryptoHandler = new CryptoHandler(cType, opeArguments(formatSize, formatSize * 2));

				if (this.opeValueHandler.containsKey(family)) {
					Map<String, CryptoHandler> temp_handlers = this.opeValueHandler.get(family);

					if (temp_handlers != null) {
						this.opeValueHandler.get(family).put(qualifier, cryptoHandler);
					}
				}
				break;
			case FPE:
				throw new UnsupportedOperationException("FPE instance does not support this operation.");
			case PLT:
			case STD:
			case DET:
			default:
				break;
		}
	}

	/**
	 * verifyOpeValueHandler() method : only used to check the OPE CryptoHandlers
	 */
	public void verifyOpeValueHandler() {
		for(String family : this.opeValueHandler.keySet()) {
			LOG.debug("Family Size: "+this.opeValueHandler.get(family).size());
			for(String qualifier : this.opeValueHandler.get(family).keySet()) {
				LOG.debug("Qualifier Properties: "+qualifier+" - "+this.opeValueHandler.get(family).get(qualifier).toString());
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
//				Check if Qualifier CryptoHandler is available. If not, return the CryptoHandler of the respective family
				if(this.opeValueHandler.containsKey(family) && this.opeValueHandler.get(family).containsKey(qualifier)) {
					return this.opeValueHandler.get(family).get(qualifier);
				} else {
					Family temp_family = this.tableSchema.getFamily(family);
					List<Object> temp_args = new ArrayList<>();
					temp_args.add(temp_family.getFormatSize());
					temp_args.add(temp_family.getFormatSize()*2);

					return new CryptoHandler(temp_family.getCryptoType(), temp_args);
				}
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
	}

	public byte[] generateCryptographicKey(CryptoTechnique.CryptoType cType) {
		switch(cType) {
			case PLT:
				return null;
			case STD:
				return stdHandler.gen();
			case DET:
				return detHandler.gen();
			case OPE:
				return opeHandler.gen();
			case FPE:
				return fpeHandler.gen();
			default:
				return null;
		}
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
	public byte[] encodeRowCryptoType(CryptoTechnique.CryptoType cType, byte[] content) {
//		byte[] row = Utils.addPadding(content, tableSchema.getKey().getFormatSize());
		byte[] row = content;
		switch (cType) {
			case PLT :
				return row;
//				return content;
			case STD :
//				LOG.debug("STD row encryption <"+this.tableSchema.getTablename()+">\n");
				return this.stdHandler.encrypt(this.stdKey, row);
//				return this.stdHandler.encrypt(this.stdKey, content);
			case DET :
//				LOG.debug("DET row encryption <"+this.tableSchema.getTablename()+">\n");
				return this.detHandler.encrypt(this.detKey, row);
//				return this.detHandler.encrypt(this.detKey, content);
			case OPE :
//				LOG.debug("OPE row encryption <"+this.tableSchema.getTablename()+">\n");
				return this.opeHandler.encrypt(this.opeKey, Utils.addPadding(content, tableSchema.getKey().getFormatSize()));
//				return this.opeHandler.encrypt(this.opeKey, content);
			case FPE :
				return this.fpeHandler.encrypt(this.fpeKey.get("KEY"), row);
//				return this.fpeHandler.encrypt(this.fpeKey.get("KEY"), content);
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
	public byte[] encodeValueCryptoType(CryptoTechnique.CryptoType cType, byte[] content, String family, String qualifier) {
//		byte[] row = Utils.addPadding(content, tableSchema.getFormatSizeFromQualifier(family, qualifier));
		byte[] row = content;
		switch (cType) {
			case PLT :
//				LOG.debug("PLT values encryption <"+this.tableSchema.getTablename()+","+family+","+qualifier+">\n");
				return row;
//				return content;
			case STD :
//				LOG.debug("STD values encryption <"+this.tableSchema.getTablename()+","+family+","+qualifier+">\n");
				return this.stdHandler.encrypt(this.stdKey, row);
//				return this.stdHandler.encrypt(this.stdKey, content);
			case DET :
//				LOG.debug("DET values encryption <"+this.tableSchema.getTablename()+","+family+","+qualifier+">\n");
				return this.detHandler.encrypt(this.detKey, row);
//				return this.detHandler.encrypt(this.detKey, content);
			case OPE :
				CryptoHandler opeCh = getCryptoHandler(CryptoTechnique.CryptoType.OPE, family, qualifier);
//				LOG.debug("OPE values encryption <"+this.tableSchema.getTablename()+","+family+","+qualifier+">\n");
				return opeCh.encrypt(this.opeKey, Utils.addPadding(content, tableSchema.getFormatSizeFromQualifier(family, qualifier)));
//				return opeCh.encrypt(this.opeKey, content);
			case FPE :
				CryptoHandler fpeCH = getCryptoHandler(CryptoTechnique.CryptoType.FPE, family, qualifier);
				return fpeCH.encrypt(this.fpeKey.get(family+":"+qualifier), row);
//				return fpeCH.encrypt(this.fpeKey.get(family+":"+qualifier), content);
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
//				byte[] unpadded = Utils.removePadding(ciphertext);
//				return unpadded;
				return ciphertext;
			case STD :
//				return Utils.removePadding(this.stdHandler.decrypt(this.stdKey, ciphertext));
//				LOG.debug("STD row decryption <"+this.tableSchema.getTablename()+">\n");
				return this.stdHandler.decrypt(this.stdKey, ciphertext);
			case DET :
//				return Utils.removePadding(this.detHandler.decrypt(this.detKey, ciphertext));
//				LOG.debug("DET row decryption <"+this.tableSchema.getTablename()+">\n");
				return this.detHandler.decrypt(this.detKey, ciphertext);
			case OPE :
//				LOG.debug("OPE row decryption <"+this.tableSchema.getTablename()+">\n");
				return Utils.removePadding(this.opeHandler.decrypt(this.opeKey, ciphertext));
//				return this.opeHandler.decrypt(this.opeKey, ciphertext);
			case FPE :
//				return Utils.removePadding(this.fpeHandler.decrypt(this.fpeKey.get("KEY"), ciphertext));
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
//				byte[] unpadded = Utils.removePadding(ciphertext);
//				return unpadded;
//				LOG.debug("PLT values decryption <"+this.tableSchema.getTablename()+","+family+","+qualifier+">\n");
				return ciphertext;
			case STD :
//				return  Utils.removePadding(this.stdHandler.decrypt(this.stdKey, ciphertext));
//				LOG.debug("STD values decryption <"+this.tableSchema.getTablename()+","+family+","+qualifier+">\n");
				byte[] std_value = this.stdHandler.decrypt(this.stdKey, ciphertext);
//				LOG.debug("STD value: "+new String(std_value));
				return std_value;
			case DET :
//				return  Utils.removePadding(this.detHandler.decrypt(this.detKey, ciphertext));
//				LOG.debug("DET values decryption <"+this.tableSchema.getTablename()+","+family+","+qualifier+">\n");
				byte[] det_value = this.detHandler.decrypt(this.detKey, ciphertext);
//				LOG.debug("DET value: "+ new String(det_value));
				return det_value;
			case OPE :
				CryptoHandler opeCh = getCryptoHandler(CryptoTechnique.CryptoType.OPE, family, qualifier);
//				LOG.debug("OPE values decryption <"+this.tableSchema.getTablename()+","+family+","+qualifier+">\n");
				byte[] ope_value = Utils.removePadding(opeCh.decrypt(this.opeKey, ciphertext));
//				LOG.debug("OPE value: "+new String(ope_value));
				return ope_value;
//				return opeCh.decrypt(this.opeKey, ciphertext);
			case FPE :
				CryptoHandler fpeCH = getCryptoHandler(CryptoTechnique.CryptoType.FPE, family, qualifier);
//				return  Utils.removePadding(fpeCH.decrypt(this.fpeKey.get(family+":"+qualifier), ciphertext));
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
		return encodeRowCryptoType(cryptoType, content);
	}

	/**
	 * decodeRow(content : byte[]) method : get the row key CryptoType and decrypt the row key
	 * @param content ciphertext row key
	 * @return call the decodeRowCryptoType method. Return the original plaintext row key in byte[] format
	 */
	public byte[] decodeRow(byte[] content) {
		CryptoTechnique.CryptoType cryptoType = this.tableSchema.getKey().getCryptoType();
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
//		If CryptoType equals to null, it means that either an error occurred in the Qualifier creation or the Qualifier
// 		instance does not exists.
		if(cryptoType == null) {
			cryptoType = this.tableSchema.getFamily(new String(family)).getCryptoType();
		}

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
		row = Utils.removePadding(row);
		String opeValues = "_STD";
		List<Cell> cellList = new ArrayList<>();

		while (res.advance()) {
			Cell cell = res.current();
			byte[] cf = CellUtil.cloneFamily(cell);

//			if(cf.length > 0) {
//				LOG.debug("CF ("+new String(cf)+")");
//			}

			byte[] cq = CellUtil.cloneQualifier(cell);

//			if(cq.length > 0) {
//				LOG.debug("CQ ("+new String(cq)+")");
//			}

			byte[] value = CellUtil.cloneValue(cell);

//			if(value.length > 0) {
//				LOG.debug("Value ("+new String(value)+")");
//			}

			long timestamp = cell.getTimestamp();
			byte type = cell.getTypeByte();

			Cell decCell;
			String qualifier = new String(cq, Charset.forName("UTF-8"));

//			Verify if the actual qualifier is equal to <qualifier>_STD
			boolean verifyProperty = false;
			if(qualifier.length() >= opeValues.length()) {
				verifyProperty = qualifier.substring(qualifier.length()-opeValues.length(), qualifier.length()).equals(opeValues);
			}

			if(!verifyProperty && cf.length > 0) {
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
//				LOG.debug("Get:Result:decodeResult:Cell: "
//						+new String(CellUtil.cloneFamily(decCell))
//						+":"+new String(CellUtil.cloneQualifier(decCell))
//						+":"+new String(CellUtil.cloneValue(decCell)));
				cellList.add(decCell);
			}
			else if(cf.length == 0) {
				decCell = CellUtil.createCell(
						row,
						cf,
						cq,
						timestamp,
						type,
						value);

//				LOG.debug("Get:Result:decodeResult:Cell: Just Row: "+decCell.toString());
				cellList.add(decCell);
			}
		}
		return Result.create(cellList);
	}

	/**
	 * getHColumnDescriptors(familiesAndQualifiers : Map<byte[], NavigableSet<byte[]>>) method : convert a mapper of the
	 * column families and the respective column qualifiers in a user friendly one
	 * @param familiesAndQualifiers received mapper
	 * @return user friendly mapper, providing the column families and the respective column qualifiers in the Map<byte[], List<byte[]>> format.
	 */
	public Map<byte[], List<byte[]>> getHColumnDescriptors(Map<byte[], NavigableSet<byte[]>> familiesAndQualifiers) {
		String opeValue = "_STD";
		Map<byte[],List<byte[]>> result = new HashMap<>();

		for(Map.Entry<byte[], NavigableSet<byte[]>> entry : familiesAndQualifiers.entrySet()) {
			if(!entry.getValue().isEmpty()) {
				Iterator i = entry.getValue().iterator();
//=======
//		for(byte[] family : familiesAndQualifiers.keySet()) {
//			NavigableSet<byte[]> q = familiesAndQualifiers.get(family);
//			if(q==null) {
//				LOG.debug(familiesAndQualifiers.toString());
//			} else
//			if (!q.isEmpty()) {
//				Iterator i = q.iterator();
//>>>>>>> lean_xcale
				List<byte[]> qualifierList = new ArrayList<>();
				while (i.hasNext()) {
					byte[] qualifier = (byte[]) i.next();
					qualifierList.add(qualifier);
					if (tableSchema.getCryptoTypeFromQualifier(new String(entry.getKey(), Charset.forName("UTF-8")), new String(qualifier, Charset.forName("UTF-8"))) == CryptoTechnique.CryptoType.OPE) {
						String q_std = new String(qualifier, Charset.forName("UTF-8"));
						qualifierList.add((q_std + opeValue).getBytes(Charset.forName("UTF-8")));
					}

				}
				result.put(entry.getKey(), qualifierList);
			}
		}

		return result;
	}

	public void dynamicHColumnDescriptorsAddition(Map<byte[], NavigableSet<byte[]>> familiesAndQualifiers, QEngineIntegration qEngine) {
		if(!familiesAndQualifiers.isEmpty()) {
			for (Map.Entry<byte[], NavigableSet<byte[]>> entry : familiesAndQualifiers.entrySet()) {
				if (!entry.getValue().isEmpty()) {
					String family = new String(entry.getKey(), Charset.forName("UTF-8"));
					Iterator i = entry.getValue().iterator();

					while (i.hasNext()) {
						byte[] temp_qualifier = (byte[]) i.next();
						String qualifier = new String(temp_qualifier, Charset.forName("UTF-8"));
						if (!qEngine.doesFamilyContainsQualifier(this.tableSchema, family, qualifier)) {
							this.tableSchema.addQualifier(family, qEngine.createDefaultQualifier(qualifier, CryptoTechnique.CryptoType.OPE));
							this.tableSchema.addQualifier(family, qEngine.createDefaultQualifier(qualifier + "_STD", CryptoTechnique.CryptoType.STD));
							replaceQualifierCryptoHandler(family, qualifier, CryptoTechnique.CryptoType.OPE, qEngine.getFamilyFormatSize());
						}
					}
				}
			}
		}
	}

	public void dynamicHColumnDescriptorsAddition(NavigableMap<byte[], List<Cell>> familiesAndQualifiers, QEngineIntegration qEngine) {
		if(!familiesAndQualifiers.isEmpty()) {
			NavigableSet<byte[]> temp_navigable_set = familiesAndQualifiers.navigableKeySet();
			Iterator i = temp_navigable_set.iterator();
			while(i.hasNext()) {
				byte[] temp_family = (byte[]) i.next();
				String family = new String(temp_family, Charset.forName("UTF-8"));
				List<Cell> temp_qualifiers = familiesAndQualifiers.get(temp_family);
				List<byte[]> qualifiers = new ArrayList<>();
				for(Cell c : temp_qualifiers) {
					byte[] temp_qualifier = CellUtil.cloneQualifier(c);
					String qualifier = new String(temp_qualifier, Charset.forName("UTF-8"));
					qualifiers.add(temp_qualifier);
					if(!qEngine.doesFamilyContainsQualifier(this.tableSchema, family, qualifier)) {
						LOG.debug("dynamicHColumnDescriptorsAddition1:"+family+":"+qualifier);
						this.tableSchema.addQualifier(family, qEngine.createDefaultQualifier(qualifier, CryptoTechnique.CryptoType.PLT));

//						this.tableSchema.addQualifier(family, qEngine.createDefaultQualifier(qualifier, CryptoTechnique.CryptoType.OPE));
//						this.tableSchema.addQualifier(family, qEngine.createDefaultQualifier(qualifier+"_STD", CryptoTechnique.CryptoType.STD));
//						replaceQualifierCryptoHandler(family, qualifier, CryptoTechnique.CryptoType.OPE, qEngine.getFamilyFormatSize());
					}
				}
			}
		}
	}

	public ByteArrayComparable checkComparatorType(ByteArrayComparable comparable, byte[] encoded_content, CryptoTechnique.CryptoType cType) {
		String comparator_name = comparable.getClass().getSimpleName();

		if(!comparator_name.isEmpty()) {
			switch (comparator_name) {
				case "BinaryPrefixComparator":
					if (cType == CryptoTechnique.CryptoType.PLT) {
						LOG.debug("New BinaryPrefixComparator created");
						return new BinaryPrefixComparator(encoded_content);
					} else {
						LOG.error("UnsupportedOperationException: BinaryPrefixComparator not supported for the current CryptoBoxes.");
						throw new UnsupportedOperationException("BinaryPrefixComparator not supported for the current CryptoBoxes.");
					}
				case "BinaryComparator":
					LOG.debug("New BinaryComparator created");
					return new BinaryComparator(encoded_content);
				default:
					LOG.error(comparator_name+" not supported for the current CyrptoBoxes.");
					throw new UnsupportedOperationException(comparator_name+" not supported for the current CryptoBoxes.");
			}
		}
		else {
			LOG.debug("NullPointerException: ByteArrayComparable object cannot be empty or null.");
			throw new NullPointerException("ByteArrayComparable object cannot be empty or null.");
		}
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

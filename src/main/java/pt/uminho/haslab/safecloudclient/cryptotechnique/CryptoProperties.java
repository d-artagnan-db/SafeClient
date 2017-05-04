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
import pt.uminho.haslab.safecloudclient.schema.Family;
import pt.uminho.haslab.safecloudclient.schema.KeyFPE;
import pt.uminho.haslab.safecloudclient.schema.Qualifier;
import pt.uminho.haslab.safecloudclient.schema.TableSchema;

import java.util.*;

/**
 * CryptoProperties class.
 * Supports the CryptoTable class with the secondary methods such as encode, decode, ...
 */
public class CryptoProperties {

	public Utils utils;
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
	public byte[] fpeKey;

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
			this.fpeHandler = new CryptoHandler(CryptoTechnique.CryptoType.FPE, this.fpeArguments());
			KeyFPE temp_key_fpe = (KeyFPE) this.tableSchema.getKey();
			this.fpeKey = temp_key_fpe.getSecurityParameters(this.fpeHandler.gen());
		}
		this.fpeValueHandler = defineFamilyCryptoHandler(CryptoTechnique.CryptoType.FPE);

		this.utils = new Utils();
	}

	/**
	 * opeArguments(formatSize : int, ciphertextSize : int) method :  set the ope initialization values.
	 * By default the OPE Cache is enabled (OpeHgd.CACHE). To disable the OPE CACHE set OpeHdg.NO_CACHE
	 * @param formatSize plaintext format size
	 * @param ciphertextSize ciphertext format size
	 * @return the ope arguments list (List<Object>)
	 */
	public List<Object> opeArguments(int formatSize, int ciphertextSize) {
		List<Object> arguments = new ArrayList<Object>();
		arguments.add(OpeHgd.CACHE);
		arguments.add(formatSize);
		arguments.add(ciphertextSize);
		return arguments;
	}


//		TODO mudar isto porque está para a key e nao para o qualifier
	public List<Object> fpeArguments() {
		List<Object> fpeArguments = new ArrayList<>();
		KeyFPE temp = (KeyFPE) this.tableSchema.getKey();
		fpeArguments.add(temp.getInstance());
		fpeArguments.add(temp.getRadix());
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
//							TODO mudar isto porque está para a key e nao para o qualifier
							ch = new CryptoHandler(cType, fpeArguments());
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
				return fpeKey;
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
				this.fpeKey = temp_key_fpe.getSecurityParameters(key);
				break;
			default :
				break;
		}
//		System.out.println("The key was setted. Key - " + Arrays.toString(key));
	}

	/**
	 * encodeRowCryptoType(cType : CryptoType, content : byte[]) method : encrypts the row key
	 * @param cType CryptoBox type
	 * @param content plaintext row key
	 * @return the resulting ciphertext
	 */
	public byte[] encodeRowCryptoType(CryptoTechnique.CryptoType cType, byte[] content) {
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
				return this.fpeHandler.encrypt(this.fpeKey, content);
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
//				TODO mudar isto - a fpeKey está c/ o tweak da key e nao do qualifier
				return fpeCH.encrypt(this.fpeKey, content);
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
	public byte[] decodeRowCryptoType(CryptoTechnique.CryptoType cType, byte[] ciphertext) {
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
				return this.fpeHandler.decrypt(this.fpeKey, ciphertext);
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
	public byte[] decodeValueCryptoType(CryptoTechnique.CryptoType cType, byte[] ciphertext, String family, String qualifier) {
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
				return fpeCH.decrypt(this.fpeKey, ciphertext);
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
		String f = new String(family);
		String q = new String(qualifier);
		CryptoTechnique.CryptoType cryptoType = this.tableSchema.getCryptoTypeFromQualifier(f, q);
		System.out.println("Encode Value (" + f + "," + q + "): " + cryptoType);
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
		String f = new String(family);
		String q = new String(qualifier);
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
			String qualifier = new String(cq);

//			Verify if the actual qualifier is equal to <qualifier>_STD
			boolean verifyProperty = false;
			if(qualifier.length() >= opeValues.length()) {
				verifyProperty = qualifier.substring(qualifier.length()-opeValues.length(), qualifier.length()).equals(opeValues);
			}

			if(!verifyProperty) {
//				If the qualifier's CryptoType equal to OPE, decrypt the auxiliary qualifier (<qualifier_STD>)
				if (tableSchema.getCryptoTypeFromQualifier(new String(cf), qualifier) == CryptoTechnique.CryptoType.OPE) {
					Cell stdCell = res.getColumnLatestCell(cf, (qualifier + opeValues).getBytes());

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
	 * isScanOrFilter(scan : Scan) method : check the CryptoType of a scan operation. It may vary if it's a Scan, Row Filter or SingleColumnValueFilter
	 * @param scan scan object used to check the instance
	 * @return the Scan's CryptoType (Row Key CryptoType in case of Scan or RowFilter, Qualifier CryptoType in case of SingleColumnValueFilter)
	 */
	public CryptoTechnique.CryptoType isScanOrFilter(Scan scan) {
		if(scan.hasFilter()) {
			Filter filter = scan.getFilter();
			if(filter instanceof RowFilter) {
				return this.tableSchema.getKey().getCryptoType();
			}
			else if(scan.getFilter() instanceof SingleColumnValueFilter) {
				SingleColumnValueFilter singleColumn = (SingleColumnValueFilter) filter;
				String family = new String(singleColumn.getFamily());
				String qualifier = new String(singleColumn.getQualifier());
				return this.tableSchema.getCryptoTypeFromQualifier(family, qualifier);
			}
			else {
				return null;
			}
		}
		else {
			return this.tableSchema.getKey().getCryptoType();
		}
	}

	/**
	 * encodeDelimitingRows(encScan : Scan, startRow : byte[], stopRow : byte[]) method : set the encrypted start and stop rows to an encrypted scan operator
	 * @param encScan encrytped scan operator
	 * @param startRow original start row
	 * @param stopRow original stop row
	 * @return an encrypted scan with the respective start and stop row, both encrypted with the row key CryptoBox
	 */
	public Scan encodeDelimitingRows(Scan encScan, byte[] startRow, byte[] stopRow) {
		if (startRow.length != 0 && stopRow.length != 0) {
			encScan.setStartRow(this.encodeRow(startRow));
			encScan.setStopRow(this.encodeRow(stopRow));
		} else if (startRow.length != 0 && stopRow.length == 0) {
			encScan.setStartRow(this.encodeRow(startRow));
		} else if (startRow.length == 0 && stopRow.length != 0) {
			encScan.setStopRow(this.encodeRow(stopRow));
		}
		return encScan;
	}

	/**
	 * encryptedScan(s : Scan) : convert a regular Scan object in the respective encrypted object
	 * @param s scan object
	 * @return the respective encrypted scan object
	 */
	public Scan encryptedScan(Scan s) {
		byte[] startRow = s.getStartRow();
		byte[] stopRow = s.getStopRow();
		Scan encScan = null;

//		get the CryptoType of the Scan/Filter operation
		CryptoTechnique.CryptoType scanCryptoType = isScanOrFilter(s);
//		Map the database column families and qualifiers into a collection
		Map<byte[], List<byte[]>>columns = this.getFamiliesAndQualifiers(s.getFamilyMap());

		switch (scanCryptoType) {
//			In case of plaintext, return the same object as received
			case PLT :
				encScan = s;
				break;
//			In case of standard or deterministic encryption, since no order is preserved a full table scan must be performed.
//			In case of Filter, the compare value must be encrypted.
			case STD :
			case DET :
			case FPE :
				encScan = new Scan();
//				Add only the specified qualifiers in the original scan (s), instead of retrieve all (unnecessary) values).
				for(byte[] f : columns.keySet()) {
					List<byte[]> qualifiersTemp = columns.get(f);
					for(byte[] q : qualifiersTemp) {
						encScan.addColumn(f, q);
					}
				}
//				Since the scanCryptoType defines the CryptoType of the scan or filter operaion, in case of SingleColumnValueFilter,
// 				the start and stop row must be encoded with the respective row key CryptoBox
				if((this.tableSchema.getKey().getCryptoType() == CryptoTechnique.CryptoType.PLT) ||
						(this.tableSchema.getKey().getCryptoType() == CryptoTechnique.CryptoType.OPE)) {
							encScan = encodeDelimitingRows(encScan, startRow, stopRow);
				}

//				In case of filter, the compare value must be encrypted
				if(s.hasFilter()) {
					if(s.getFilter() instanceof SingleColumnValueFilter) {
//						System.out.println("Entrou no singlecolumn cenas");
						SingleColumnValueFilter f = (SingleColumnValueFilter) s.getFilter();
						ByteArrayComparable bComp = f.getComparator();
						byte[] value = bComp.getValue();

						encScan.setFilter(new SingleColumnValueFilter(f.getFamily(), f.getQualifier(), f.getOperator(), this.encodeRow(value)));
//						System.out.println("Fez set Filter");
					}
				}

				break;
			case OPE :
				encScan = new Scan();
//				Add only the specified qualifiers in the original scan (s), instead of retrieve all (unnecessary) values).
				for(byte[] f : columns.keySet()) {
					List<byte[]> qualifiersTemp = columns.get(f);
					for(byte[] q : qualifiersTemp) {
						encScan.addColumn(f, q);
					}
				}
//				Since the scanCryptoType defines the CryptoType of the scan or filter operaion, in case of SingleColumnValueFilter,
// 				the start and stop row must be encoded with the respective row key CryptoBox
				if((this.tableSchema.getKey().getCryptoType() == CryptoTechnique.CryptoType.PLT) ||
						(this.tableSchema.getKey().getCryptoType() == CryptoTechnique.CryptoType.OPE)) {
					encScan = encodeDelimitingRows(encScan, startRow, stopRow);
				}
				if (s.hasFilter()) {
					Filter encryptedFilter = (Filter) parseFilter(s.getFilter());
					encScan.setFilter(encryptedFilter);
				}
				break;
			default :
				break;
		}
		return encScan;
	}

	/**
	 * parseFilter(filter : Filter) method : when setting a filter, parse it and handle it according the respective CryptoType
	 * @param filter Filter object
	 * @return provide an encrypted Filter, with the respective operator and compare value.
	 */
	public Object parseFilter(Filter filter) {
		CompareFilter.CompareOp comp;
		ByteArrayComparable bComp;
		Object returnValue = null;

		if(filter != null) {
			if(filter instanceof RowFilter) {
				RowFilter rowFilter = (RowFilter) filter;
				comp = rowFilter.getOperator();
				bComp = rowFilter.getComparator();

				switch (this.tableSchema.getKey().getCryptoType()) {
					case PLT :
						returnValue = rowFilter;
						break;
					case STD :
					case DET :
					case FPE :
						Object[] parserResult = new Object[2];
						parserResult[0] = comp;
						parserResult[1] = bComp.getValue();

						returnValue = parserResult;
						break;
					case OPE :
//						Generate a Binary Comparator to perform the comparison with the respective encrypted value
						BinaryComparator encBC = new BinaryComparator(this.encodeRow(bComp.getValue()));
						returnValue = new RowFilter(comp, encBC);
						break;
					default:
						returnValue = null;
						break;
				}
			}
			else if(filter instanceof SingleColumnValueFilter) {
				SingleColumnValueFilter singleFilter = (SingleColumnValueFilter) filter;
				byte[] family = singleFilter.getFamily();
				byte[] qualifier = singleFilter.getQualifier();
				comp = singleFilter.getOperator();
				bComp = singleFilter.getComparator();

				switch (this.tableSchema.getCryptoTypeFromQualifier(new String(family), new String(qualifier))) {
					case PLT :
						returnValue = singleFilter;
						break;
					case STD :
					case DET :
					case FPE :
						Object[] parserResult = new Object[4];
						parserResult[0] = family;
						parserResult[1] = qualifier;
						parserResult[2] = comp;
						parserResult[3] = bComp.getValue();

						returnValue = parserResult;
						break;
					case OPE :
//						Generate a Binary Comparator to perform the comparison with the respective encrypted value
						BinaryComparator encBC = new BinaryComparator(this.encodeValue(family, qualifier, bComp.getValue()));
						returnValue = new SingleColumnValueFilter(family, qualifier, comp, encBC);
						break;
					default:
						returnValue = null;
						break;
				}
			}
		}

		return returnValue;
	}

	/**
	 * verifyFilterCryptoType(scan : Scan) method : verify the filter's CryptoBox
	 * @param scan scan/filter object
	 * @return the respective CryptoType
	 */
	public CryptoTechnique.CryptoType verifyFilterCryptoType(Scan scan) {
		CryptoTechnique.CryptoType cryptoType = this.tableSchema.getKey().getCryptoType();

		if(scan.hasFilter()) {
			Filter filter = scan.getFilter();
			if(filter instanceof SingleColumnValueFilter) {
				String family = new String(((SingleColumnValueFilter) filter).getFamily());
				String qualifier = new String(((SingleColumnValueFilter)filter).getQualifier());
				cryptoType = this.tableSchema.getCryptoTypeFromQualifier(family, qualifier);
			}
		}
		return cryptoType;
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
					if (tableSchema.getCryptoTypeFromQualifier(new String(family), new String(qualifier)) == CryptoTechnique.CryptoType.OPE) {
						String q_std = new String(qualifier);
						qualifierList.add((q_std + opeValue).getBytes());
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
				temp = tweak.getBytes();
			case "FF3" :
				byte[] temp_tweak = tweak.getBytes();
				if(temp_tweak.length == 8) {
					temp = temp_tweak;
				}
				else if(temp_tweak.length > 8) {
					System.arraycopy(temp_tweak, 0, temp, 0, 8);
				}
				else {
					throw new IllegalArgumentException("For an FF3 instance, the tweak must have a 64bit length");
				}
		}
		return temp;
	}

}

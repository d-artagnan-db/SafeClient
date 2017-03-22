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
import pt.uminho.haslab.safecloudclient.schema.Qualifier;
import pt.uminho.haslab.safecloudclient.schema.TableSchema;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by rgmacedo on 2/22/17.
 */
public class CryptoProperties {

	public Utils utils;
	public TableSchema tableSchema;

	public CryptoHandler stdHandler;
	public CryptoHandler detHandler;
	public CryptoHandler opeHandler;
	public Map<String, Map<String, CryptoHandler>> opeValueHandler;

	public byte[] stdKey;
	public byte[] detKey;
	public byte[] opeKey;

	public CryptoProperties(TableSchema ts) {
		this.tableSchema = ts;

		this.stdHandler = new CryptoHandler(CryptoTechnique.CryptoType.STD, new ArrayList<>());
		this.stdKey = this.stdHandler.gen();

		this.detHandler = new CryptoHandler(CryptoTechnique.CryptoType.DET, new ArrayList<>());
		this.detKey = this.detHandler.gen();

		this.opeHandler = new CryptoHandler(CryptoTechnique.CryptoType.OPE, opeArguments(23, 46));
		this.opeKey = this.opeHandler.gen();

		this.opeValueHandler = defineFamilyCryptoHandler();
		this.verifyOpeValueHandler();

		this.utils = new Utils();
	}

	public List<Object> opeArguments(int formatSize, int ciphertextSize) {
		List<Object> arguments = new ArrayList<Object>();
		arguments.add(OpeHgd.CACHE);
		arguments.add(formatSize);
		arguments.add(ciphertextSize);
		return arguments;
	}

	public Map<String, Map<String, CryptoHandler>> defineFamilyCryptoHandler() {
		Map<String, Map<String, CryptoHandler>> familyCryptoHandler = new HashMap<>();
		for(Family f : this.tableSchema.getColumnFamilies()) {
			Map<String, CryptoHandler> qualifierCryptoHandler = new HashMap<>();
			for(Qualifier q : f.getQualifiers()) {
				if(q.getCryptoType().equals(CryptoTechnique.CryptoType.OPE)) {
					qualifierCryptoHandler.put(
							q.getName(),
							new CryptoHandler(
									CryptoTechnique.CryptoType.OPE,
									opeArguments(q.getFormatSize(), q.getFormatSize()*2)));
				}
			}
			familyCryptoHandler.put(f.getFamilyName(), qualifierCryptoHandler);
		}
		return familyCryptoHandler;
	}

	public void verifyOpeValueHandler() {
		for(String family : this.opeValueHandler.keySet()) {
			System.out.println("Family Size: "+this.opeValueHandler.get(family).size());
			for(String qualifier : this.opeValueHandler.get(family).keySet()) {
				System.out.println("Qualifier Properties: "+qualifier+" - "+this.opeValueHandler.get(family).get(qualifier).toString());
			}
		}
	}

	public CryptoHandler getCryptoHandler(String family, String qualifier) {
		return this.opeValueHandler.get(family).get(qualifier);
	}

	/**
	 * Get Encryption/Decryption Key from CryptoHandler
	 * 
	 * @return
	 */
	public byte[] getKey(CryptoTechnique.CryptoType cType) {
		switch (cType) {
			case STD :
				return stdKey;
			case DET :
				return detKey;
			case OPE :
				return opeKey;
			default :
				return null;
		}
	}

	/**
	 * Set the Encryption/Decryption Key in the CryptoHandler
	 * 
	 * @param key
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
			default :
				break;
		}
		System.out.println("The key was setted. Key - " + Arrays.toString(key));
	}

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
			default :
				return null;
		}
	}

	public byte[] encodeValueCryptoType(CryptoTechnique.CryptoType cType, byte[] content, String family, String qualifier) {
		switch (cType) {
			case PLT :
				return content;
			case STD :
				return this.stdHandler.encrypt(this.stdKey, content);
			case DET :
				return this.detHandler.encrypt(this.detKey, content);
			case OPE :
				CryptoHandler opeCh = getCryptoHandler(family, qualifier);
				return opeCh.encrypt(this.opeKey, content);
			default :
				return null;
		}
	}

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
			default :
				return null;
		}
	}

	public byte[] decodeValueCryptoType(CryptoTechnique.CryptoType cType, byte[] ciphertext, String family, String qualifier) {
		switch (cType) {
			case PLT :
				return ciphertext;
			case STD :
				return this.stdHandler.decrypt(this.stdKey, ciphertext);
			case DET :
				return this.detHandler.decrypt(this.detKey, ciphertext);
			case OPE :
				CryptoHandler opeCh = getCryptoHandler(family, qualifier);
				return opeCh.decrypt(this.opeKey, ciphertext);
			default :
				return null;
		}
	}

	/**
	 * Encode a given content, apart the CryptoType
	 * 
	 * @param content
	 * @return
	 */
	public byte[] encodeRow(byte[] content) {
		CryptoTechnique.CryptoType cryptoType = this.tableSchema.getKey().getCryptoType();
		System.out.println("Encode Row: " + cryptoType);
		return encodeRowCryptoType(cryptoType, content);
	}

	/**
	 * Decode a given content, apart the CryptoType
	 * 
	 * @param content
	 * @return
	 */
	public byte[] decodeRow(byte[] content) {
		CryptoTechnique.CryptoType cryptoType = this.tableSchema.getKey().getCryptoType();
		System.out.println("Decode Row: " + cryptoType);
		return decodeRowCryptoType(cryptoType, content);
	}

	public byte[] encodeValue(byte[] family, byte[] qualifier, byte[] value) {
		String f = new String(family);
		String q = new String(qualifier);
		CryptoTechnique.CryptoType cryptoType = this.tableSchema.getCryptoTypeFromQualifer(f, q);
		System.out.println("Encode Value (" + f + "," + q + "): " + cryptoType);
		return encodeValueCryptoType(cryptoType, value, f, q);
	}

	public byte[] decodeValue(byte[] family, byte[] qualifier, byte[] value) {
		String f = new String(family);
		String q = new String(qualifier);
		CryptoTechnique.CryptoType cryptoType = this.tableSchema.getCryptoTypeFromQualifer(f, q);
		System.out.println("Decode Value (" + f + "," + q + "): " + cryptoType);
		return decodeValueCryptoType(cryptoType, value, f, q);
	}

	/**
	 * Decode a Result given a row (key) and an encrypted result (value). Return
	 * the respective value decrypted.
	 * 
	 * @param row
	 * @param res
	 * @return
	 */
	public Result decodeResult(byte[] row, Result res) {
		byte[] decodedRow = this.decodeRow(row);
		List<Cell> cellList = new ArrayList<Cell>();

		while (res.advance()) {
			Cell cell = res.current();
			byte[] cf = CellUtil.cloneFamily(cell);
			byte[] cq = CellUtil.cloneQualifier(cell);
			byte[] value = CellUtil.cloneValue(cell);
			long timestamp = cell.getTimestamp();
			byte type = cell.getTypeByte();

			Cell decCell = CellUtil.createCell(
					decodedRow,
					cf,
					cq,
					timestamp,
					type,
					this.decodeValue(cf, cq, value));
			cellList.add(decCell);
		}
		return Result.create(cellList);
	}

	/**
	 * Convert a Scan operation in the respective Encrypted operation
	 * 
	 * @param s
	 * @return
	 */
	public Scan encryptedScan(Scan s) {
		byte[] startRow = s.getStartRow();
		byte[] stopRow = s.getStopRow();
		Scan encScan = null;

		switch (this.tableSchema.getKey().getCryptoType()) {
			case PLT :
				encScan = s;
				break;
			case STD :
			case DET :
				encScan = new Scan();
				break;
			case OPE :
				encScan = new Scan();
				if (startRow.length != 0 && stopRow.length != 0) {
					encScan.setStartRow(this.encodeRow(startRow));
					encScan.setStopRow(this.encodeRow(stopRow));
				} else if (startRow.length != 0 && stopRow.length == 0) {
					encScan.setStartRow(this.encodeRow(startRow));
				} else if (startRow.length == 0 && stopRow.length != 0) {
					encScan.setStopRow(this.encodeRow(stopRow));
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
	 * When setting a filter, parse it and handle it according the respective
	 * CryptoType
	 * 
	 * @param filter
	 * @return
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
					case PLT:
						returnValue = rowFilter;
						break;
					case STD:
					case DET:
						Object[] parserResult = new Object[2];
						parserResult[0] = comp;
						parserResult[1] = bComp.getValue();

						returnValue = parserResult;
						break;
					case OPE:
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

				switch (this.tableSchema.getCryptoTypeFromQualifer(new String(family), new String(qualifier))) {
					case PLT:
						returnValue = singleFilter;
						break;
					case STD:
					case DET:
						Object[] parserResult = new Object[4];
						parserResult[0] = family;
						parserResult[1] = qualifier;
						parserResult[2] = comp;
						parserResult[3] = bComp.getValue();

						returnValue = parserResult;
						break;
					case OPE:
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


	public CryptoTechnique.CryptoType verifyFilterCryptoType(Scan scan) {
		CryptoTechnique.CryptoType cryptoType = this.tableSchema.getKey().getCryptoType();

		if(scan.hasFilter()) {
			Filter filter = scan.getFilter();
			if(filter instanceof SingleColumnValueFilter) {
				String family = new String(((SingleColumnValueFilter) filter).getFamily());
				String qualifier = new String(((SingleColumnValueFilter)filter).getQualifier());
				cryptoType = this.tableSchema.getCryptoTypeFromQualifer(family, qualifier);
			}
		}
		return cryptoType;
	}

	// TODO remove temporary Method
	/**
	 * This is only a temporary Method
	 * 
	 * @param filename
	 * @return
	 * @throws IOException
	 */
	public static byte[] readKeyFromFile(String filename) throws IOException {
		FileInputStream stream = new FileInputStream(filename);
		try {
			byte[] key = new byte[stream.available()];
			int b;
			int i = 0;

			while ((b = stream.read()) != -1) {
				key[i] = (byte) b;
				i++;
			}
			System.out.println("readKeyFromFile: " + Arrays.toString(key));

			return key;
		} catch (Exception e) {
			System.out.println("Exception. " + e.getMessage());
		} finally {
			stream.close();
		}
		return null;
	}

}

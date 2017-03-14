package pt.uminho.haslab.safecloudclient.cryptotechnique;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import pt.uminho.haslab.cryptoenv.CryptoHandler;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.cryptoenv.Utils;
import pt.uminho.haslab.safecloudclient.schema.TableSchema;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by rgmacedo on 2/22/17.
 */
public class CryptoProperties {

	// public CryptoHandler handler;
	// public CryptoTechnique.CryptoType cType;
	// public byte[] key;
	public Utils utils;
	// public int formatSize;
	//
	public TableSchema tableSchema;

	public CryptoHandler stdHandler;
	public CryptoHandler detHandler;
	public CryptoHandler opeHandler;

	public byte[] stdKey;
	public byte[] detKey;
	public byte[] opeKey;

	public CryptoProperties(TableSchema ts) {
		this.tableSchema = ts;
		this.stdHandler = new CryptoHandler(CryptoTechnique.CryptoType.STD);
		this.stdKey = this.stdHandler.gen();

		this.detHandler = new CryptoHandler(CryptoTechnique.CryptoType.DET);
		this.detKey = this.detHandler.gen();

		this.opeHandler = new CryptoHandler(CryptoTechnique.CryptoType.OPE);
		this.opeKey = this.opeHandler.gen();

		this.utils = new Utils();
	}

	// public CryptoProperties(CryptoTechnique.CryptoType cType, int formatSize)
	// {
	// this.cType = cType;
	// this.handler = new CryptoHandler(cType);
	// this.key = this.handler.gen();
	// this.utils = new Utils();
	// this.formatSize = formatSize;
	// }

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

	/**
	 * Encode a given content, apart the CryptoType
	 * 
	 * @param content
	 * @return
	 */
	public byte[] encodeRow(byte[] content) {
		// TODO mudar isto - retirar o if e por um PLT no CryptoType
		if (this.tableSchema.getKey().getCryptoType() != null) {
			switch (this.tableSchema.getKey().getCryptoType()) {
				case STD :
					return this.stdHandler.encrypt(this.stdKey, content);
				case DET :
					return this.detHandler.encrypt(this.detKey, content);
				case OPE :
					return this.opeHandler.encrypt(this.opeKey, content);
				default :
					return content;
			}
		} else
			return content;
		// return this.handler.encrypt(this.key, content);
	}

	/**
	 * Decode a given content, apart the CryptoType
	 * 
	 * @param content
	 * @return
	 */
	public byte[] decodeRow(byte[] content) {
		// TODO mudar isto - retirar o if e por um PLT no CryptoType
		if (this.tableSchema.getKey().getCryptoType() != null) {
			switch (this.tableSchema.getKey().getCryptoType()) {
				case STD :
					return this.stdHandler.decrypt(this.stdKey, content);
				case DET :
					return this.detHandler.decrypt(this.detKey, content);
				case OPE :
					return this.opeHandler.decrypt(this.opeKey, content);
				default :
					return content;
			}
		} else
			return content;
		// return this.handler.decrypt(this.key, content);
	}

	public byte[] encodeValue(byte[] family, byte[] qualifier, byte[] value) {
		String f = new String(family);
		String q = new String(qualifier);
		switch (this.tableSchema.getCryptoTypeFromQualifer(f, q)) {
			case STD :
				return this.stdHandler.encrypt(this.stdKey, value);
			case DET :
				return this.detHandler.encrypt(this.detKey, value);
			case OPE :
				return this.opeHandler.encrypt(this.opeKey, value);
			default :
				return value;
		}
	}

	public byte[] decodeValue(byte[] family, byte[] qualifier, byte[] value) {
		String f = new String(family);
		String q = new String(qualifier);
		switch (this.tableSchema.getCryptoTypeFromQualifer(f, q)) {
			case STD :
				return this.stdHandler.decrypt(this.stdKey, value);
			case DET :
				return this.detHandler.decrypt(this.detKey, value);
			case OPE :
				return this.opeHandler.decrypt(this.opeKey, value);
			default :
				return value;
		}
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
		List<Cell> cellList = new ArrayList<Cell>();
		while (res.advance()) {
			Cell cell = res.current();
			byte[] cf = CellUtil.cloneFamily(cell);
			byte[] cq = CellUtil.cloneQualifier(cell);
			byte[] value = CellUtil.cloneValue(cell);
			long timestamp = cell.getTimestamp();
			byte type = cell.getTypeByte();

			Cell decCell = CellUtil.createCell(this.decodeRow(row), cf, cq,
					timestamp, type, this.decodeValue(cf, cq, value));
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
					RowFilter encryptedFilter = (RowFilter) parseFilter((RowFilter) s
							.getFilter());
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
	public Object parseFilter(RowFilter filter) {
		CompareFilter.CompareOp comp;
		ByteArrayComparable bComp;

		if (filter != null) {
			switch (this.tableSchema.getKey().getCryptoType()) {
				case STD :
				case DET :
					comp = filter.getOperator();
					bComp = filter.getComparator();

					Object[] parserResult = new Object[2];
					parserResult[0] = comp;
					parserResult[1] = bComp.getValue();

					return parserResult;
				case OPE :
					comp = filter.getOperator();
					bComp = filter.getComparator();
					BinaryComparator encBC = new BinaryComparator(
							this.encodeRow(bComp.getValue()));

					return new RowFilter(comp, encBC);
				default :
					return null;
			}
		} else
			return null;
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

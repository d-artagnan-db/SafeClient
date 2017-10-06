package pt.uminho.haslab.safecloudclient.schema;

import org.apache.commons.codec.digest.Crypt;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties.whichFpeInstance;

/**
 * Family class.
 * Holds all the relevant information associated to a specific Family.
 */
public class Family {

	private ByteBuffer familyName;
	private CryptoTechnique.CryptoType cryptoType;
	private int formatSize;
//	TODO: perhaps this attribute needs to be changed to a Map<byte[],Qualifier> in order to perform a faster search (O(1) vs O(N));
	private List<Qualifier> qualifiers;
	private Boolean columnPadding;

	public Family() {
		this.familyName = null;
		this.cryptoType = CryptoTechnique.CryptoType.PLT;
		this.formatSize = 0;
		this.qualifiers = new ArrayList<>();
		this.columnPadding = null;
	}

//	TEST-ME
	public Family(ByteBuffer familyName, CryptoTechnique.CryptoType cType, int formatSize, Boolean columnPadding) {
//        this.familyName = familyName;
        this.familyName = familyName.duplicate();
		this.cryptoType = cType;
        this.formatSize = formatSize;
        this.qualifiers = new ArrayList<>();
        this.columnPadding = columnPadding;
    }

//    FIXME: if I am not using this constructor, delete it
    public Family(ByteBuffer familyName, CryptoTechnique.CryptoType cType, int formatSize, Boolean columnPadding, List<Qualifier> quals) {
		this.familyName = familyName.duplicate();
		this.cryptoType = cType;
		this.formatSize = formatSize;
		this.columnPadding = columnPadding;
		this.qualifiers = quals;
	}


	public ByteBuffer getFamilyName() {
		return this.familyName;
	}

	public CryptoTechnique.CryptoType getCryptoType() {
		return this.cryptoType;
	}

	public int getFormatSize() {
		return this.formatSize;
	}

	public Boolean getColumnPadding() {
		return this.columnPadding;
	}

//	TEST-ME
	public List<Qualifier> getQualifiers() {
		List<Qualifier> qualifiersTemp = new ArrayList<Qualifier>();
		qualifiersTemp.addAll(this.qualifiers);
		return qualifiersTemp;
	}


	public void setFamilyName(ByteBuffer familyName) {
		this.familyName.clear();
		this.familyName = familyName.duplicate();
	}

	public void setCryptoType(CryptoTechnique.CryptoType cType) {
		this.cryptoType = cType;
	}

	public void setFormatSize(int formatSize) {
		this.formatSize = formatSize;
	}

	public void setColumnPadding(Boolean columnPadding) {
		this.columnPadding = columnPadding;
	}

//	TEST-ME
	public void setQualifiers(List<Qualifier> qualifiers) {
		this.qualifiers = new ArrayList<>();
		this.qualifiers.addAll(qualifiers);
	}


	/**
	 * addQualifier(qualifierName : String, cryptoType : CryptoType, formatSize : int, properties : Map<String,String>) method :
	 * add a new qualifier to the Qualifiers list. If both format size and CryptoType are undefined, the qualifier assume
	 * the default properties (inherited from the column family)
	 * @param qualifierName column qualifier
	 * @param cryptoType CryptoBox type
	 * @param formatSize size of qualifier
	 */
	public void addQualifier(ByteBuffer qualifierName, CryptoTechnique.CryptoType cryptoType, int formatSize, Boolean columnPadding, Map<String,String> properties) {
		CryptoTechnique.CryptoType cType;
		int fSize = 0;
		Boolean padding = false;

		if (cryptoType != null)
			cType = cryptoType;
		else
			cType = this.cryptoType;

		if (formatSize > 0) {
			fSize = formatSize;
		} else {
			fSize = this.formatSize;
		}

		if (columnPadding != null) {
			padding = columnPadding;
		} else {
			padding = this.columnPadding;
		}

		this.qualifiers.add(new Qualifier(qualifierName, cType, fSize, padding, properties));
	}

	/**
	 * addQualifier(qualifier : Qualifier) method : add a new qualifier to the Qualifiers list. If both format size and
	 * CryptoType are undefined, the qualifier assume the default properties (inherited from the column family)
	 * @param qualifier Qualifier object
	 */
	public void addQualifier(Qualifier qualifier) {
		if (qualifier instanceof QualifierFPE) {
			QualifierFPE q = new QualifierFPE();
			QualifierFPE qTemp = (QualifierFPE) qualifier;

			if(qualifier.getName() != null)
				q.setQualifierName(qualifier.getName());

			if (qualifier.getCryptoType() == null)
				q.setCryptoType(this.cryptoType);
			else
				q.setCryptoType(qualifier.getCryptoType());

			if (qualifier.getFormatSize() == 0)
				q.setFormatSize(this.formatSize);
			else
				q.setFormatSize(qualifier.getFormatSize());

			if (qualifier.getPadding() == null)
				q.setPadding(this.columnPadding);
			else
				q.setPadding(qualifier.getPadding());

			if (qTemp.getInstance() != null) {
				q.setInstance(qTemp.getInstance());
				q.setFpeInstance(whichFpeInstance(qTemp.getInstance()));
			}

			if(qTemp.getRadix() > 0)
				q.setRadix(qTemp.getRadix());

			if(qTemp.getTweak() != null)
				q.setTweak(qTemp.getTweak());

			this.qualifiers.add(q);
		}
		else {
			if (qualifier.getCryptoType() == null)
				qualifier.setCryptoType(this.cryptoType);

			if (qualifier.getFormatSize() == 0)
				qualifier.setFormatSize(this.formatSize);

			if (qualifier.getPadding() == null) {
				qualifier.setPadding(this.columnPadding);
			}

			this.qualifiers.add(qualifier);
		}
	}




	/**
	 * containsQualifier(qualifier :String) method : verify if qualifier List contains a given qualifier
	 * @param qualifier column qualifier
	 * @return true if qualifier exist. Otherwise false.
	 */
	public boolean containsQualifier(ByteBuffer qualifier) {
		boolean contains = false;
		for (Qualifier q : this.qualifiers) {
			if (q.getName().compareTo(qualifier) == 0) {
				contains = true;
				break;
			}
		}
		return contains;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Family Name: ").append(this.familyName.toString()).append("\n");
		sb.append("Family CryptoType: ").append(this.cryptoType).append("\n");
		sb.append("Family FormatSize: ").append(this.formatSize).append("\n");
		sb.append("Family Padding: ").append(this.columnPadding).append("\n");
		sb.append("Column Qualifiers: \n");
		for (Qualifier q : this.qualifiers) {
			sb.append(q.toString());
		}
		return sb.toString();
	}
}

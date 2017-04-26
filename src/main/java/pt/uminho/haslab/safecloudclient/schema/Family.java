package pt.uminho.haslab.safecloudclient.schema;

import org.apache.commons.codec.digest.Crypt;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Family class.
 * Holds all the relevant information associated to a specific Family.
 */
public class Family {

	private String familyName;
	private CryptoTechnique.CryptoType cryptoType;
	private int formatSize;
	private List<Qualifier> qualifiers;

	public Family() {
		this.familyName = "";
		this.cryptoType = CryptoTechnique.CryptoType.PLT;
		this.formatSize = 0;
		this.qualifiers = new ArrayList<>();
	}

	public Family(String familyName, CryptoTechnique.CryptoType cType, int formatSize) {
        this.familyName = familyName;
        this.cryptoType = cType;
        this.formatSize = formatSize;
        this.qualifiers = new ArrayList<>();
    }

    public Family(String familyName, CryptoTechnique.CryptoType cType, int formatSize, List<Qualifier> quals) {
		this.familyName = familyName;
		this.cryptoType = cType;
		this.formatSize = formatSize;
		this.qualifiers = quals;
	}

	public String getFamilyName() {
		return this.familyName;
	}

	public CryptoTechnique.CryptoType getCryptoType() {
		return this.cryptoType;
	}

	public int getFormatSize() {
		return this.formatSize;
	}

	public List<Qualifier> getQualifiers() {
		List<Qualifier> qualifiersTemp = new ArrayList<Qualifier>();
		for (Qualifier q : this.qualifiers)
			qualifiersTemp.add(q);
		return qualifiersTemp;
	}

	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}

	public void setCryptoType(CryptoTechnique.CryptoType cType) {
		this.cryptoType = cType;
	}

	public void setFormatSize(int formatSize) {
		this.formatSize = formatSize;
	}

	public void setQualifiers(List<Qualifier> qualifiers) {
		this.qualifiers = new ArrayList<Qualifier>();
		for (Qualifier q : qualifiers) {
			this.qualifiers.add(q);
		}
	}

	/**
	 * addQualifier(qualifierName : String, cryptoType : CryptoType, formatSize : int, properties : Map<String,String>) method :
	 * add a new qualifier to the Qualifiers list. If both format size and CryptoType are undefined, the qualifier assume
	 * the default properties (inherited from the column family)
	 * @param qualifierName column qualifier
	 * @param cryptoType CryptoBox type
	 * @param formatSize size of qualifier
	 */
	public void addQualifier(String qualifierName, CryptoTechnique.CryptoType cryptoType, int formatSize, Map<String,String> properties) {
		CryptoTechnique.CryptoType cType;
		int fSize = 0;

		if (cryptoType != null)
			cType = cryptoType;
		else
			cType = this.cryptoType;

		if (formatSize > 0) {
			fSize = formatSize;
//			System.out.println("Entrou na primeira condição - " + fSize + " - "
//					+ formatSize);
		} else {
			fSize = this.formatSize;
//			System.out.println("Entrou na segunda condição - " + fSize + " - "
//					+ this.formatSize);
		}

		this.qualifiers.add(new Qualifier(qualifierName, cType, fSize, properties));
	}

	/**
	 * addQualifier(qualifier : Qualifier) method : add a new qualifier to the Qualifiers list. If both format size and
	 * CryptoType are undefined, the qualifier assume the default properties (inherited from the column family)
	 * @param qualifier Qualifier object
	 */
	public void addQualifier(Qualifier qualifier) {
		if (qualifier.getCryptoType() == null)
			qualifier.setCryptoType(this.cryptoType);

		if (qualifier.getFormatSize() == 0)
			qualifier.setFormatSize(this.formatSize);

		this.qualifiers.add(qualifier);
	}

	/**
	 * containsQualifier(qualifier :String) method : verify if qualifier List contains a given qualifier
	 * @param qualifier column qualifier
	 * @return true if qualifier exist. Otherwise false.
	 */
	public boolean containsQualifier(String qualifier) {
		boolean contains = false;
		for (Qualifier q : this.qualifiers) {
			if (q.getName().equals(qualifier)) {
				contains = true;
				break;
			}
		}
		return contains;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Family Name: ").append(familyName).append("\n");
		sb.append("Family CryptoType: ").append(cryptoType).append("\n");
		sb.append("Family FormatSize: ").append(formatSize).append("\n");
		sb.append("Column Qualifiers: \n");
		for (Qualifier q : this.qualifiers) {
			sb.append(q.toString());
		}
		return sb.toString();
	}
}

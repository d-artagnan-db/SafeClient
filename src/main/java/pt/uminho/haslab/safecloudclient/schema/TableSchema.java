package pt.uminho.haslab.safecloudclient.schema;

import com.sun.tools.javac.util.Name;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;

import java.util.*;

import static pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoProperties.whichFpeInstance;

/**
 * TableSchema class.
 * Mapper of the database schema provided by the user.
 */
public class TableSchema {
	private String tablename;
//	Default Row Key CryptoBox
	private CryptoTechnique.CryptoType defaultKeyCryptoType;
//	Default Qualifiers CryptoBox
	private CryptoTechnique.CryptoType defaultColumnsCryptoType;
//	Default format size for both row key and values
	private int defaultFormatSize;

//	Key object. Contains CryptoBox, formatSize and other information about the Row Key
	private Key key;
//  Collection of the database column families (and qualifiers)
	private List<Family> columnFamilies;

	public TableSchema() {
		this.tablename = "";
		this.defaultKeyCryptoType = CryptoTechnique.CryptoType.PLT;
		this.defaultColumnsCryptoType = CryptoTechnique.CryptoType.PLT;
		this.defaultFormatSize = 0;
		this.key = new Key();
		this.columnFamilies = new ArrayList<>();
	}

	public TableSchema(String tablename, CryptoTechnique.CryptoType defKey, CryptoTechnique.CryptoType defColumns, int defFormat, Key key, List<Family> families) {
		this.tablename = tablename;
		this.defaultKeyCryptoType = defKey;
		this.defaultColumnsCryptoType = defColumns;
		this.defaultFormatSize = defFormat;
		this.key = key;
		this.columnFamilies = families;
	}

	public String getTablename() {
		return this.tablename;
	}

	public CryptoTechnique.CryptoType getDefaultKeyCryptoType() {
		return this.defaultKeyCryptoType;
	}

	public CryptoTechnique.CryptoType getDefaultColumnsCryptoType() {
		return this.defaultColumnsCryptoType;
	}

	public int getDefaultFormatSize() {
		return this.defaultFormatSize;
	}

	public Key getKey() {
		return this.key;
	}

	public List<Family> getColumnFamilies() {
		List<Family> tempFamilies = new ArrayList<Family>();

		for (Family family : this.columnFamilies)
			tempFamilies.add(family);

		return tempFamilies;
	}

	public void setTablename(String tablename) {
		this.tablename = tablename;
	}

	public void setDefaultKeyCryptoType(CryptoTechnique.CryptoType cType) {
		this.defaultKeyCryptoType = cType;
	}

	public void setDefaultColumnsCryptoType(CryptoTechnique.CryptoType cType) {
		this.defaultColumnsCryptoType = cType;
	}

	public void setDefaultFormatSize(int formatSize) {
		this.defaultFormatSize = formatSize;
	}

	public void setKey(Key key) {
		if(key instanceof KeyFPE) {
			KeyFPE temp = new KeyFPE();
			if (key.getCryptoType() == null)
				temp.setCryptoType(this.defaultKeyCryptoType);
			else
				temp.setCryptoType(key.getCryptoType());

			if (key.getFormatSize() <= 0)
				temp.setFormatSize(this.defaultFormatSize);
			else
				temp.setFormatSize(key.getFormatSize());

			if (((KeyFPE) key).getInstance() != null) {
				temp.setInstance(((KeyFPE) key).getInstance());
				temp.setFpeInstance(whichFpeInstance(((KeyFPE) key).getInstance()));
			}

			if(((KeyFPE) key).getRadix() > 0)
				temp.setRadix(((KeyFPE) key).getRadix());

			if(((KeyFPE) key).getTweak() != null)
				temp.setTweak(((KeyFPE) key).getTweak());

			this.key = temp;
		}
		else {
			if (key.getCryptoType() == null)
				this.key.setCryptoType(this.defaultKeyCryptoType);
			else
				this.key.setCryptoType(key.getCryptoType());

			if (key.getFormatSize() <= 0)
				this.key.setFormatSize(this.defaultFormatSize);
			else
				this.key.setFormatSize(key.getFormatSize());
		}
	}

	public void setColumnFamilies(List<Family> families) {
		this.columnFamilies = new ArrayList<Family>();

		for (Family family : families)
			this.columnFamilies.add(family);
	}

	/**
	 * addFamily(familyName : String, cType : CryptoType, formatSize : int, qualifiers : List<Qualifier>) method : add a new column family to the database mapper
	 * Parametrized version.
	 * @param familyName column family name
	 * @param cType CryptoBox type
	 * @param formatSize column family default size
	 * @param qualifiers set of column qualifiers
	 */
	public void addFamily(String familyName, CryptoTechnique.CryptoType cType, int formatSize, List<Qualifier> qualifiers) {
		Family family = new Family();
		family.setFamilyName(familyName);

		if (cType == null)
			family.setCryptoType(defaultColumnsCryptoType);
		else
			family.setCryptoType(cType);

		if (formatSize <= 0)
			family.setFormatSize(defaultFormatSize);
		else
			family.setFormatSize(formatSize);

		for (Qualifier q : qualifiers)
			family.addQualifier(q);

		this.columnFamilies.add(family);
	}

	/**
	 * addFamily(fam : Family) method : add a new column family to the database mapper
	 * Object version.
	 * @param fam Family object
	 */
	public void addFamily(Family fam) {
		if (fam.getCryptoType() == null)
			fam.setCryptoType(defaultColumnsCryptoType);

		if (fam.getFormatSize() <= 0)
			fam.setFormatSize(defaultFormatSize);

		this.columnFamilies.add(fam);
	}

	public Family getFamily(String familyName) {
		Family wantedFamily = null;
		boolean hasFamily = false;
		Iterator<Family> family_iterator = this.columnFamilies.iterator();
		while(family_iterator.hasNext() && !hasFamily) {
			Family temp_family = family_iterator.next();
			if(temp_family.getFamilyName().equals(familyName)) {
				wantedFamily = temp_family;
				hasFamily = true;
			}
		}

		return wantedFamily;
	}

	public boolean containsFamily(String family) {
		boolean contains = false;
		for(Family f : this.columnFamilies) {
			if(f.getFamilyName().equals(family)) {
				contains = true;
				break;
			}
		}
		return contains;
	}

	/**
	 * addQualifier(familyName : String, qualifier : Qualifier) method : add a new column qualifier to the respective family collection
	 * @param familyName column family name
	 * @param qualifier Qualifier object.
	 */
	public void addQualifier(String familyName, Qualifier qualifier) {
		int index = 0;

		for (Family f : this.columnFamilies) {
			if (f.getFamilyName().equals(familyName)) {
				index = this.columnFamilies.indexOf(f);
				break;
			}
		}

		if (index > -1) {
			Family f = this.columnFamilies.get(index);
			f.addQualifier(qualifier);
			this.columnFamilies.set(index, f);
		}
	}

	public boolean containsQualifier(String family, String qualifier) {
		boolean contains = false;
		if (containsFamily(family)) {
			if(getFamily(family).containsQualifier(qualifier)) {
				contains = true;
			}
		}

		return contains;
	}

	/**
	 * getCryptoTypeFromQualifier(family : String, qualifier : String)  method : get the CryptoType of a given family:qualifier
	 * @param family column family
	 * @param qualifier column qualifier
	 * @return the respective CryptoType
	 */
	public CryptoTechnique.CryptoType getCryptoTypeFromQualifier(String family, String qualifier) {
		CryptoTechnique.CryptoType cType = null;
		for (Family f : this.columnFamilies) {
			if (f.getFamilyName().equals(family)) {
				for (Qualifier q : f.getQualifiers()) {
					if (q.getName().equals(qualifier)) {
						cType = q.getCryptoType();
						break;
					}
				}
				break;
			}
		}
		if (cType == null) {
			throw new NullPointerException("The specified qualifier does not exists.");
		} else {
			return cType;
		}
	}

//	public CryptoTechnique.CryptoType getCryptoTypeFromQualifier(String family, String qualifier) {
//		CryptoTechnique.CryptoType cType = null;
//		Iterator<Family> family_iterator = this.columnFamilies.iterator();
//		boolean catched = false;
//		while(family_iterator.hasNext() && !catched) {
//			Family temp_family = family_iterator.next();
//			if(temp_family.getFamilyName().equals(family)) {
//				Iterator<Qualifier> qualifier_iterator = temp_family.getQualifiers().iterator();
//				while(qualifier_iterator.hasNext() && !catched) {
//					Qualifier temp_qualifier = qualifier_iterator.next();
//					if(temp_qualifier.getName().equals(qualifier)) {
//						catched = true;
//						cType = temp_qualifier.getCryptoType();
//					}
//				}
//			}
//		}
//		return cType;
//	}

	/**
	 * getGeneratorTypeFromQualifier(family : String, qualifier : String)  method : get the generator type of a given family:qualifier (e.g., String, Date, Integer)
	 * @param family column family
	 * @param qualifier column qualifier
	 * @return the respective Generator in string format
	 */
	public String getGeneratorTypeFromQualifier(String family, String qualifier) {
		String gen = null;
		for(Family f : this.columnFamilies) {
			if (f.getFamilyName().equals(family)) {
				for(Qualifier q : f.getQualifiers()) {
					if (q.getName().equals(qualifier)) {
						gen = q.getProperties().get("GENERATOR");
						break;
					}
				}
				break;
			}
		}
		return gen;
	}

	/**
	 * getFormatSizeFromQualifier(family : String, qualifier : String)  method : get the FormatSize of a given family:qualifier
	 * @param family column family
	 * @param qualifier column qualifier
	 * @return the respective format size in Integer format
	 */
	public Integer getFormatSizeFromQualifier(String family, String qualifier) {
		int formatSize = 0;
		for (Family f : this.columnFamilies) {
			if (f.getFamilyName().equals(family)) {
				for (Qualifier q : f.getQualifiers()) {
					if (q.getName().equals(qualifier)) {
						formatSize = q.getFormatSize();
						break;
					}
				}
				break;
			}
		}
		return formatSize;
	}

	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Table Schema\n");
		sb.append("Table Name: ").append(this.tablename).append("\n");
		sb.append("Default Schema: \n");
		sb.append("> Default Key CryptoType: " + defaultKeyCryptoType).append(
				"\n");
		sb.append("> Default Columns CryptoType: " + defaultColumnsCryptoType)
				.append("\n");
		sb.append("> Default Format Size CryptoType: " + defaultFormatSize)
				.append("\n");
		sb.append("Key CryptoType: ").append(this.key.toString()).append("\n");
		sb.append("Columns: \n");
		for (Family family : this.columnFamilies) {
			sb.append("> Family: ").append(family.toString()).append("\n");
		}

		return sb.toString();
	}

}

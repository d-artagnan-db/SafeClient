package pt.uminho.haslab.safecloudclient.schema;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.dom4j.*;
import org.dom4j.io.SAXReader;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;
import pt.uminho.haslab.safecloudclient.cryptotechnique.CryptoTable;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * SchemaParser class.
 * Used to parse the database schema file.
 */
public class SchemaParser {
	static final Log LOG = LogFactory.getLog(CryptoTable.class.getName());
	public Map<String,TableSchema> tableSchemas;
	private HBaseAdmin admin;

	private CryptoTechnique.CryptoType defaultPropertiesKey;
	private CryptoTechnique.CryptoType defaultPropertiesColumns;
	private Boolean defaultPropertiesKeyPadding;
	private Boolean defaultPropertiesColumnPadding;
	private int defaultPropertiesKeyFormatSize;
	private int defaultPropertiesColFormatSize;



	public SchemaParser(Configuration conf) {
		this.tableSchemas = new HashMap<>();
		try {
			this.admin = new HBaseAdmin(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Map<String, TableSchema> getSchemas() {
		return this.tableSchemas;
	}

	public TableSchema getTableSchema(String tablename) {
		if (this.tableSchemas.containsKey(tablename)) {
			return this.tableSchemas.get(tablename);
		} else {
			return null;
		}
	}

	public Map<String, Object> getDatabaseDefaultProperties() {
		Map<String, Object> properties = new HashMap<>();
		properties.put("defaultPropertiesKey",this.defaultPropertiesKey);
		properties.put("defaultPropertiesColumns", this.defaultPropertiesColumns);
		properties.put("defaultPropertiesKeyPadding", this.defaultPropertiesKeyPadding);
		properties.put("defaultPropertiesColumnPadding", this.defaultPropertiesColumnPadding);
		properties.put("defaultPropertiesKeyFormatSize", this.defaultPropertiesKeyFormatSize);
		properties.put("defaultPropertiesColFormatSize", this.defaultPropertiesColFormatSize);

		return properties;
	}

	/**
	 * parse(filename : String) method : parse the database schema file (<schema>.xml)
	 * @param filename database schema path
	 */
	public void parseDatabaseTables(String filename) {
		try {
//			Read schema file
			File inputFile = new File(filename);
			SAXReader reader = new SAXReader();
			Document document = reader.read(inputFile);


//			Map the schema file into an Element object
			Element rootElement = document.getRootElement();

			parseDatabaseDefaultProperties(rootElement.element("default"), new TableSchema());

			List<Element> tables = rootElement.elements("table");
			for(Element table_element : tables) {
				TableSchema temp_schema = parseTable(table_element);
				this.tableSchemas.put(temp_schema.getTablename(), temp_schema);
			}


		} catch (DocumentException e) {
			LOG.error("CryptoWorker:SchemaParser:parse:DocumentException:"+e.getMessage());
		}
	}


	public void parseDatabaseDefaultProperties(Element rootElement, TableSchema tableSchema) {
		System.out.println("ParseDatabaseDefaultProperties:");

		if(rootElement != null) {
			String key = rootElement.elementText("key");
			String cols = rootElement.elementText("columns");
			String keyPadding = rootElement.elementText("keypadding");
			String colPadding = rootElement.elementText("colpadding");
			String keySize = rootElement.elementText("keyformatsize");
			String colSize = rootElement.elementText("colformatsize");

			if (key == null || key.isEmpty()) {
				throw new NullPointerException("CryptoWorker:SchemaParser:parseDatabaseDefaultProperties:Default row key Cryptographic Type cannot be null nor empty.");
			}
			if (cols == null || cols.isEmpty()) {
				throw new NullPointerException("CryptoWorker:SchemaParser:parseDatabaseDefaultProperties:Default columns Cryptographic Type cannot be null nor empty.");
			}
			if (keyPadding == null || keyPadding.isEmpty()) {
				throw new NullPointerException("CryptoWorker:SchemaParser:parseDatabaseDefaultProperties:Default key padding cannot be null nor empty.");
			}
			if (colPadding == null || colPadding.isEmpty()) {
				throw new NullPointerException("CryptoWorker:SchemaParser:parseDatabaseDefaultProperties:Default columns padding cannot be null nor empty.");
			}
			if (keySize == null || keySize.isEmpty()) {
				throw new NullPointerException("CryptoWorker:SchemaParser:parseDatabaseDefaultProperties:Default key format size cannot be null nor empty.");
			}
			if (colSize == null || colSize.isEmpty()) {
				throw new NullPointerException("CryptoWorker:SchemaParser:parseDatabaseDefaultProperties:Default columns format size cannot be null nor empty.");
			}

			this.defaultPropertiesKey = switchCryptoType(key);
			this.defaultPropertiesColumns = switchCryptoType(cols);
			this.defaultPropertiesKeyPadding = paddingBooleanConvertion(keyPadding);
			this.defaultPropertiesColumnPadding = paddingBooleanConvertion(colPadding);
			this.defaultPropertiesKeyFormatSize = formatSizeIntegerValue(keySize);
			this.defaultPropertiesColFormatSize = formatSizeIntegerValue(colSize);
		}
		else {
			throw new NullPointerException("SchemaParser:parseDatabaseDefaultProperties:Default element cannot be null.");
		}
	}

	public TableSchema generateDefaultTableSchema(String tablename) {
		TableSchema temp = new TableSchema();
		temp.setTablename(tablename);

//		Set default key properties
//		Set default Column properties
//		Get HColumnDescriptor

		return null;
	}

	public TableSchema parseTable(Element rootElement) {
		TableSchema ts = new TableSchema();

		HColumnDescriptor[] hColumnDescriptors = parseTablename(rootElement, ts);
		parseTableDefaultProperties(rootElement, ts);
		parseKey(rootElement, ts);
		parseColumns(rootElement, ts, hColumnDescriptors);

		return ts;
	}



	/**
	 * parseTablename(rootElement : Element) method : parse the table name
	 * @param rootElement main Element node
	 */
	public HColumnDescriptor[] parseTablename(Element rootElement, TableSchema tableSchema) {
		Element nameElement = rootElement.element("name");
		String name = nameElement.getText();
		if(name == null || name.isEmpty()) {
			throw new NullPointerException("CryptoWorker:SchemaParser:parseTablename:Table name cannot be null nor empty.");
		}

		tableSchema.setTablename(name);
		HTableDescriptor descriptor = getHColumnDescriptors(TableName.valueOf(name));
		if(descriptor != null) {
			return descriptor.getColumnFamilies();
		}
		else {
			return null;
		}
	}

	/**
	 * parseDefault(rootElement : Element) method : parse the default database parameters
	 * @param rootElement main Element node
	 */
	public void parseTableDefaultProperties(Element rootElement, TableSchema tableSchema) {
		Element defaultElement = rootElement.element("default");
		if (defaultElement != null) {
			String key = defaultElement.elementText("key");
			String columns = defaultElement.elementText("columns");
			String keyformatsize = defaultElement.elementText("keyformatsize");
			String colformatsize = defaultElement.elementText("colformatsize");
			String keypadding = defaultElement.elementText("keypadding");
			String colpadding = defaultElement.elementText("colpadding");

			if (key == null || key.isEmpty()) {
				tableSchema.setDefaultKeyCryptoType(this.defaultPropertiesKey);
			} else {
				tableSchema.setDefaultKeyCryptoType(switchCryptoType(key));
			}

			if (columns == null || columns.isEmpty()) {
				tableSchema.setDefaultColumnsCryptoType(this.defaultPropertiesColumns);
			} else {
				tableSchema.setDefaultColumnsCryptoType(switchCryptoType(columns));
			}

			if (keyformatsize == null || keyformatsize.isEmpty()) {
				tableSchema.setDefaultKeyFormatSize(this.defaultPropertiesKeyFormatSize);
			} else {
				tableSchema.setDefaultKeyFormatSize(formatSizeIntegerValue(keyformatsize));
			}

			if (colformatsize == null || colformatsize.isEmpty()) {
				tableSchema.setDefaultColumnFormatSize(this.defaultPropertiesColFormatSize);
			} else {
				tableSchema.setDefaultColumnFormatSize(formatSizeIntegerValue(colformatsize));
			}

			if (keypadding == null || keypadding.isEmpty()) {
				tableSchema.setDefaultKeyPadding(this.defaultPropertiesKeyPadding);
			} else {
				tableSchema.setDefaultKeyPadding(paddingBooleanConvertion(keypadding));
			}

			if (colpadding == null || colpadding.isEmpty()) {
				tableSchema.setDefaultColumnPadding(this.defaultPropertiesColumnPadding);
			} else {
				tableSchema.setDefaultColumnPadding(paddingBooleanConvertion(colpadding));
			}
		}
		else {
			throw new NullPointerException("CryptoWorker:SchemaParser:parseDefault:Default arguments specification cannot be null nor empty.");
		}
	}

	/**
	 * parseKey(rootElement : Element) method : parse the key properties from the database schema
	 * @param rootElement main Element node
	 */
	public void parseKey(Element rootElement, TableSchema tableSchema) {
		Element keyElement = rootElement.element("key");
		if (keyElement != null) {
			String cryptotechnique = keyElement.elementText("cryptotechnique");
			String formatsize = keyElement.elementText("formatsize");
			String keypadding = keyElement.elementText("keypadding");

			String instance = keyElement.elementText("instance");
			String radix = keyElement.elementText("radix");
			String tweak = keyElement.elementText("tweak");

			if(cryptotechnique == null || cryptotechnique.isEmpty()) {
				cryptotechnique = tableSchema.getDefaultKeyCryptoType().toString();
			}

			if(formatsize == null || formatsize.isEmpty()) {
				formatsize = String.valueOf(tableSchema.getDefaultKeyFormatSize());
			}

			if(keypadding == null || keypadding.isEmpty()) {
				keypadding = String.valueOf(tableSchema.getDefaultKeyPadding());
			}

			if(cryptotechnique.equals("FPE")) {
				validateFPEArguments(instance, radix, tweak);
			}

			if(!cryptotechnique.equals("FPE")) {
				Key key = new Key(switchCryptoType(cryptotechnique), formatSizeIntegerValue(formatsize), paddingBooleanConvertion(keypadding));
				tableSchema.setKey(key);
			}
			else {
				Key key = new KeyFPE(
						switchCryptoType(cryptotechnique),
						formatSizeIntegerValue(formatsize),
						paddingBooleanConvertion(keypadding),
						instance,
						radixIntegerValue(radix),
						tweak);
				tableSchema.setKey(key);
			}
		}
		else {
//			If key arguments are not specified in schema file create key with the default values
			tableSchema.setKey(new Key(tableSchema.getDefaultKeyCryptoType(), tableSchema.getDefaultKeyFormatSize(), tableSchema.getDefaultKeyPadding()));
		}
	}

	/**
	 * parseColumns(rootElement : Element) method : parse the column families and qualifiers properties from the database schema
	 * @param rootElement main Element node
	 */
	public void parseColumns(Element rootElement, TableSchema tableSchema, HColumnDescriptor[] hColumnDescriptors) {
		boolean hasDescriptor = false;
		Element columnsElement = rootElement.element("columns");
		if(columnsElement == null) {
			throw new NoSuchElementException("Columns arguments cannot be null.");
		}

		List<Element> familiesElement = columnsElement.elements("family");
		for (Element family : familiesElement) {
			if (family != null) {
				String familyName = family.elementText("name");
				String familyCryptoTechnique = family.elementText("cryptotechnique");
				String familyFormatSize = family.elementText("colformatsize");
				String familyPadding = family.elementText("colpadding");

				if(familyName == null || familyName.isEmpty()) {
					throw new NullPointerException("Column Family name cannot be null nor empty.");
				}

				if(familyCryptoTechnique == null || familyCryptoTechnique.isEmpty()) {
					familyCryptoTechnique = tableSchema.getDefaultColumnsCryptoType().toString();
				}

				if(familyFormatSize == null || familyFormatSize.isEmpty()) {
					familyFormatSize = String.valueOf(tableSchema.getDefaultColumnFormatSize());
				}

				if(familyPadding == null || familyPadding.isEmpty()) {
					familyPadding = String.valueOf(tableSchema.getDefaultColumnPadding());
				}

//				if(hColumnDescriptors != null && hColumnDescriptors.length > 0) {
//					hasDescriptor = true;
//				}
//
//				if(hasDescriptor && hasHColumnDescriptor(hColumnDescriptors, familyName)) {
//					System.out.println("HasDescriptor: " + Arrays.toString(hColumnDescriptors));
//				}
//				else if(!hasDescriptor) {
//					System.out.println("HasDescriptor = false");
					Family f = new Family(
							familyName,
							switchCryptoType(familyCryptoTechnique),
							formatSizeIntegerValue(familyFormatSize),
							paddingBooleanConvertion(familyPadding));

					tableSchema.addFamily(f);

					List<Element> qualifiersElement = family.elements("qualifier");
					for (Element qualifier : qualifiersElement) {
						String qualifierName = qualifier.elementText("name");
						String cryptotechniqueQualifier = qualifier.elementText("cryptotechnique");
						String qualifierFormatsize = qualifier.elementText("colformatsize");
						String qualifierPadding = qualifier.elementText("colpadding");

						String instance = qualifier.elementText("instance");
						String radix = qualifier.elementText("radix");
						String tweak = qualifier.elementText("tweak");

						List<Element> misc = qualifier.elements("misc");
						Map<String, String> properties = parseMiscellaneous(misc);

						if (qualifierName == null || qualifierName.isEmpty()) {
							throw new NullPointerException("Column qualifier name cannot be null nor empty.");
						}

						if (cryptotechniqueQualifier == null || cryptotechniqueQualifier.isEmpty()) {
							cryptotechniqueQualifier = familyCryptoTechnique;
						}

						if (qualifierFormatsize == null || qualifierFormatsize.isEmpty()) {
							qualifierFormatsize = familyFormatSize;
						}

						if (qualifierPadding == null || qualifierPadding.isEmpty()) {
							qualifierPadding = familyPadding;
						}

						if (cryptotechniqueQualifier.equals("FPE")) {
							validateFPEArguments(instance, radix, tweak);
						}

						Qualifier q;
						if (!cryptotechniqueQualifier.equals("FPE")) {
							q = new Qualifier(
									qualifierName,
									switchCryptoType(cryptotechniqueQualifier),
									formatSizeIntegerValue(qualifierFormatsize),
									paddingBooleanConvertion(qualifierPadding),
									properties);

						} else {
							q = new QualifierFPE(
									qualifierName,
									switchCryptoType(cryptotechniqueQualifier),
									formatSizeIntegerValue(qualifierFormatsize),
									paddingBooleanConvertion(qualifierPadding),
									properties,
									instance,
									radixIntegerValue(radix),
									tweak
							);
						}

						tableSchema.addQualifier(familyName, q);

						if (cryptotechniqueQualifier.equals("OPE")) {
							String stdQualifierName = qualifierName + "_STD";
							String stdCType = "STD";

							Qualifier std = new Qualifier(
									stdQualifierName,
									switchCryptoType(stdCType),
									formatSizeIntegerValue(qualifierFormatsize),
									paddingBooleanConvertion(qualifierPadding),
									properties
							);

							tableSchema.addQualifier(familyName, std);
						}

					}
				}
//				else {
//					throw new NullPointerException("The column family "+familyName+", specified in the schema file does not match with the HColumnDescriptors:");
//
//				}
//			}
//			else {
//				throw new NoSuchElementException("Column family element cannot be null nor empty.");
//			}
		}
	}


	/**
	 * parseMiscellaneous(properties : List<Element>) method : parse random properties from the database schema
	 * @param properties list of Element nodes
	 * @return a mapper of the property and the type in Map<String,String> format
	 */
	public Map<String,String> parseMiscellaneous(List<Element> properties) {
		Map<String, String > result = new HashMap<>();
		for(Element property : properties) {
			result.put(property.elementText("property"), property.elementText("type"));
		}
		return result;
	}

	public CryptoTechnique.CryptoType switchCryptoType(String cType) {
		if (cType == null)
			return null;
		else
			switch (cType) {
				case "STD" :
					return CryptoTechnique.CryptoType.STD;
				case "DET" :
					return CryptoTechnique.CryptoType.DET;
				case "OPE" :
					return CryptoTechnique.CryptoType.OPE;
				case "FPE" :
					return CryptoTechnique.CryptoType.FPE;
				case "PLT" :
				default :
					return CryptoTechnique.CryptoType.PLT;
			}
	}

	private int formatSizeIntegerValue(String formatSize) {
		int value;
		if (formatSize == null || formatSize.isEmpty())
			value = 0;
		else {
			try {
				value = Integer.parseInt(formatSize);
			} catch (NumberFormatException e) {
				LOG.error("SchemaParser:formatSizeIntegerValue:NumberFormatException:"+e.getMessage());
				value = 0;
			}
		}
		return value;
	}

	private Boolean paddingBooleanConvertion(String padding) {
		Boolean value;
		if (padding == null || padding.isEmpty()) {
			throw new NullPointerException("SchemaParser:paddingBooleanConvertion:Boolean Value cannot be null nor empty.");
		} else {
			value = Boolean.valueOf(padding);
		}
		return value;
	}

	public int radixIntegerValue(String radix) {
		if (radix == null || radix.isEmpty())
			return 10;
		else
			return Integer.parseInt(radix);
	}

	public void validateFPEArguments(String instance, String radix, String tweak) {
		if(instance == null || instance.isEmpty()) {
			throw new NullPointerException("Format-Preserving Encryption instance cannot be null nor empty.");
		}

		if(radix == null || radix.isEmpty()) {
			throw new NullPointerException("Format-Preserving Encryption radix cannot be null nor empty.");
		}

		if(tweak == null) {
			throw new NullPointerException("Format-Preserving Encryption tweak cannot be null.");
		}
	}

	public String printDatabaseSchemas() {
		StringBuilder sb = new StringBuilder();
		for(String schema : tableSchemas.keySet()) {
			sb.append("---------------------------\n");
			sb.append(tableSchemas.get(schema).toString());
		}
		return sb.toString();
	}

	public boolean hasHColumnDescriptor(HColumnDescriptor[] hColumnDescriptors, String descriptor) {
		boolean col = false;
		System.out.println("Size: "+hColumnDescriptors.length);
		for(int i = 0; i < hColumnDescriptors.length && !col; i++) {
			if(hColumnDescriptors[i].getNameAsString().equals(descriptor)) {
				col = true;
			}
			else {
				i++;
			}
		}
		return col;
	}

	public HTableDescriptor getHColumnDescriptors(TableName tablename) {
		HTableDescriptor descriptor = null;
		try {
			if(this.admin.tableExists(tablename)) {
				descriptor = this.admin.getTableDescriptor(tablename);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return descriptor;
	}
}

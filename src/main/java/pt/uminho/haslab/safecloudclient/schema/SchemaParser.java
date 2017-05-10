package pt.uminho.haslab.safecloudclient.schema;

import org.dom4j.*;
import org.dom4j.io.SAXReader;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SchemaParser class.
 * Used to parse the database schema file.
 */
public class SchemaParser {
	public Map<String,TableSchema> tableSchemas;

	public SchemaParser() {
		this.tableSchemas = new HashMap<>();
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

	/**
	 * parse(filename : String) method : parse the database schema file (<schema>.xml)
	 * @param filename database schema path
	 */
	public void parse(String filename) {
		try {
			long starttime = System.currentTimeMillis();

//			Read schema file
			File inputFile = new File(filename);
			SAXReader reader = new SAXReader();
			Document document = reader.read(inputFile);

//			System.out.println("Root Element: " + document.getRootElement().getName());
//			Map the schema file into an Element object
			Element rootElement = document.getRootElement();

			List<Element> tables = rootElement.elements("table");
			for(Element table_element : tables) {
				TableSchema temp_schema = parseSchema(table_element);
				this.tableSchemas.put(temp_schema.getTablename(), temp_schema);
			}


			long stopttime = System.currentTimeMillis();
//			System.out.println("Parsing Time: " + (stopttime - starttime));
		} catch (DocumentException e) {
			System.out.println("DocumentException - "+e.getMessage());
		}
	}


	public TableSchema parseSchema(Element rootElement) {
		TableSchema ts = new TableSchema();

		parseTablename(rootElement, ts);
		parseDefault(rootElement, ts);
		parseKey(rootElement, ts);
		parseColumns(rootElement, ts);

		return ts;
	}

	/**
	 * parseTablename(rootElement : Element) method : parse the table name
	 * @param rootElement main Element node
	 */
	public void parseTablename(Element rootElement, TableSchema tableSchema) {
		Element nameElement = rootElement.element("name");
		String name = nameElement.getText();
		if (name != null) {
			tableSchema.setTablename(name);
		}
	}

	/**
	 * parseDefault(rootElement : Element) method : parse the default database parameters
	 * @param rootElement main Element node
	 */
	public void parseDefault(Element rootElement, TableSchema tableSchema) {
		Element defaultElement = rootElement.element("default");
		if (defaultElement != null) {
			String key = defaultElement.elementText("key");
			String columns = defaultElement.elementText("columns");
			String formatsize = defaultElement.elementText("formatsize");

			if (key != null) {
				tableSchema.setDefaultKeyCryptoType(switchCryptoType(key));
			}
			if (columns != null) {
				tableSchema.setDefaultColumnsCryptoType(switchCryptoType(columns));
			}
			if (formatsize != null) {
				tableSchema.setDefaultFormatSize(formatSizeIntegerValue(formatsize));
			}
		}
	}

	/**
	 * parseKey(rootElement : Element) method : parse the key properties from the database schema
	 * @param rootElement main Element node
	 */
	public void parseKey(Element rootElement, TableSchema tableSchema) {
		Element keyElement = rootElement.element("key");
		if (keyElement != null) {
			String formatsize = keyElement.elementText("formatsize");
			String cryptotechnique = keyElement.elementText("cryptotechnique");
			String instance = keyElement.elementText("instance");
			String radix = keyElement.elementText("radix");
			String tweak = keyElement.elementText("tweak");

			if(!cryptotechnique.equals("FPE")) {
				Key key = new Key(switchCryptoType(cryptotechnique), formatSizeIntegerValue(formatsize));
				tableSchema.setKey(key);
			}
			else {
				Key key = new KeyFPE(
						switchCryptoType(cryptotechnique),
						formatSizeIntegerValue(formatsize),
						instance,
						radixIntegerValue(radix),
						tweak);
				tableSchema.setKey(key);
			}
		}
	}

	/**
	 * parseColumns(rootElement : Element) method : parse the column families and qualifiers properties from the database schema
	 * @param rootElement main Element node
	 */
	public void parseColumns(Element rootElement, TableSchema tableSchema) {
		Element columnsElement = rootElement.element("columns");
		if (columnsElement != null) {
			List<Element> familiesElement = columnsElement.elements("family");
			for (Element family : familiesElement) {
				if (family != null) {
					String familyName = family.elementText("name");
					String cryptotechnique = family.elementText("cryptotechnique");
					String formatsize = family.elementText("formatsize");

					Family f = new Family(
							familyName,
							switchCryptoType(cryptotechnique),
							formatSizeIntegerValue(formatsize));

					tableSchema.addFamily(f);

					List<Element> qualifiersElement = family.elements("qualifier");
					for (Element qualifier : qualifiersElement) {
						String qualifierName = qualifier.elementText("name");
						String cryptotechniqueQualifier = qualifier.elementText("cryptotechnique");
						String qualifierFormatsize = qualifier.elementText("formatsize");
						String instance = qualifier.elementText("instance");
						String radix = qualifier.elementText("radix");
						String tweak = qualifier.elementText("tweak");

						List<Element> misc = qualifier.elements("misc");
						Map<String,String> properties = parseMiscellaneous(misc);

						Qualifier q;
						if (!cryptotechniqueQualifier.equals("FPE")) {
							q = new Qualifier(
									qualifierName,
									switchCryptoType(cryptotechniqueQualifier),
									formatSizeIntegerValue(qualifierFormatsize),
									properties);

						}
						else {
							q = new QualifierFPE(
									qualifierName,
									switchCryptoType(cryptotechniqueQualifier),
									formatSizeIntegerValue(qualifierFormatsize),
									properties,
									instance,
									radixIntegerValue(radix),
									tweak
							);
						}

						tableSchema.addQualifier(familyName, q);

						if(cryptotechniqueQualifier.equals("OPE")) {
							String stdQualifierName = qualifierName+"_STD";
							String stdCType = "STD";

							Qualifier std = new Qualifier(
									stdQualifierName,
									switchCryptoType(stdCType),
									formatSizeIntegerValue(qualifierFormatsize),
									properties
							);

							tableSchema.addQualifier(familyName, std);
						}

					}
				}
			}
		}
	}

//		TODO falta por o default

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

	public int formatSizeIntegerValue(String formatSize) {
		if (formatSize == null || formatSize.isEmpty())
			return 0;
		else
			return Integer.parseInt(formatSize);
	}

	public int radixIntegerValue(String radix) {
		if (radix == null || radix.isEmpty())
			return 10;
		else
			return Integer.parseInt(radix);
	}

	public String printDatabaseSchemas() {
		StringBuilder sb = new StringBuilder();
		for(String schema : tableSchemas.keySet()) {
			sb.append("---------------------------\n");
			sb.append(tableSchemas.get(schema).toString());
		}
		return sb.toString();
	}
}

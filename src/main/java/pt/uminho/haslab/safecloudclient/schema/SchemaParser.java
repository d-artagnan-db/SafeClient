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
	public TableSchema tableSchema;

	public SchemaParser() {
		this.tableSchema = new TableSchema();
	}

	public TableSchema getTableSchema() {
		return this.tableSchema;
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

			parseTablename(rootElement);
			parseDefault(rootElement);
			parseKey(rootElement);
			parseColumns(rootElement);

			long stopttime = System.currentTimeMillis();
//			System.out.println("Parsing Time: " + (stopttime - starttime));
		} catch (DocumentException e) {
			System.out.println("DocumentException - "+e.getMessage());
		}
	}

	/**
	 * parseTablename(rootElement : Element) method : parse the table name
	 * @param rootElement main Element node
	 */
	public void parseTablename(Element rootElement) {
		Element nameElement = rootElement.element("name");
		String name = nameElement.getText();
		if (name != null) {
			this.tableSchema.setTablename(name);
		}
	}

	/**
	 * parseDefault(rootElement : Element) method : parse the default database parameters
	 * @param rootElement main Element node
	 */
	public void parseDefault(Element rootElement) {
		Element defaultElement = rootElement.element("default");
		if (defaultElement != null) {
			String key = defaultElement.elementText("key");
			String columns = defaultElement.elementText("columns");
			String formatsize = defaultElement.elementText("formatsize");

			if (key != null) {
				this.tableSchema.setDefaultKeyCryptoType(switchCryptoType(key));
			}
			if (columns != null) {
				this.tableSchema
						.setDefaultColumnsCryptoType(switchCryptoType(columns));
			}
			if (formatsize != null) {
				this.tableSchema
						.setDefaultFormatSize(formatSizeIntegerValue(formatsize));
			}
		}
	}

	/**
	 * parseKey(rootElement : Element) method : parse the key properties from the database schema
	 * @param rootElement main Element node
	 */
	public void parseKey(Element rootElement) {
		Element keyElement = rootElement.element("key");
		if (keyElement != null) {
			String formatsize = keyElement.elementText("formatsize");
			String cryptotechnique = keyElement.elementText("cryptotechnique");
			String instance = keyElement.elementText("instance");
			String radix = keyElement.elementText("radix");
			String tweak = keyElement.elementText("tweak");

			if(!cryptotechnique.equals("FPE")) {
				Key key = new Key(switchCryptoType(cryptotechnique), formatSizeIntegerValue(formatsize));
				this.tableSchema.setKey(key);
			}
			else {
				Key key = new KeyFPE(
						switchCryptoType(cryptotechnique),
						formatSizeIntegerValue(formatsize),
						instance,
						radixIntegerValue(radix),
						tweak);
				this.tableSchema.setKey(key);
			}
		}
	}

	/**
	 * parseColumns(rootElement : Element) method : parse the column families and qualifiers properties from the database schema
	 * @param rootElement main Element node
	 */
	public void parseColumns(Element rootElement) {
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

					this.tableSchema.addFamily(f);

					List<Element> qualifiersElement = family.elements("qualifier");
					for (Element qualifier : qualifiersElement) {
						String qualifierName = qualifier.elementText("name");
						String cryptotechniqueQualifier = qualifier.elementText("cryptotechnique");
						String qualifierFormatsize = qualifier.elementText("formatsize");

						List<Element> misc = qualifier.elements("misc");
						Map<String,String> properties = parseMiscellaneous(misc);

						Qualifier q = new Qualifier(
								qualifierName,
								switchCryptoType(cryptotechniqueQualifier),
								formatSizeIntegerValue(qualifierFormatsize),
								properties);

						this.tableSchema.addQualifier(familyName, q);

						if(cryptotechniqueQualifier.equals("OPE")) {
							String stdQualifierName = qualifierName+"_STD";
							String stdCType = "STD";

							Qualifier std = new Qualifier(
									stdQualifierName,
									switchCryptoType(stdCType),
									formatSizeIntegerValue(qualifierFormatsize),
									properties
							);

							this.tableSchema.addQualifier(familyName, std);
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

}

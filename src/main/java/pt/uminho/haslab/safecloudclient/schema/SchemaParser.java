package pt.uminho.haslab.safecloudclient.schema;

import org.dom4j.*;
import org.dom4j.io.SAXReader;
import pt.uminho.haslab.cryptoenv.CryptoTechnique;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rgmacedo on 3/13/17.
 */
public class SchemaParser {
	public TableSchema tableSchema;

	public SchemaParser() {
		this.tableSchema = new TableSchema();
	}

	public TableSchema getTableSchema() {
		return this.tableSchema;
	}

	public void parse(String filename) {
		try {
			long starttime = System.currentTimeMillis();

			File inputFile = new File(filename);
			SAXReader reader = new SAXReader();
			Document document = reader.read(inputFile);

			System.out.println("Root Element: "
					+ document.getRootElement().getName());

			Element rootElement = document.getRootElement();

			parseTablename(rootElement);
			parseDefault(rootElement);
			parseKey(rootElement);
			parseColumns(rootElement);

			long stopttime = System.currentTimeMillis();
			System.out.println("Parsing Time: " + (stopttime - starttime));
		} catch (DocumentException e) {
			e.printStackTrace();
		}
	}

	public void parseTablename(Element rootElement) {
		Element nameElement = rootElement.element("name");
		String name = nameElement.getText();
		if (name != null) {
			this.tableSchema.setTablename(name);
		}
	}

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

	public void parseKey(Element rootElement) {
		Element keyElement = rootElement.element("key");
		if (keyElement != null) {
			String formatsize = keyElement.elementText("formatsize");
			String cryptotechnique = keyElement.elementText("cryptotechnique");

			Key key = new Key(switchCryptoType(cryptotechnique),
					formatSizeIntegerValue(formatsize));

			this.tableSchema.setKey(key);

		}
	}

	public void parseColumns(Element rootElement) {
		Element columnsElement = rootElement.element("columns");
		if (columnsElement != null) {
			List<Element> familiesElement = columnsElement.elements("family");
			for (Element family : familiesElement) {
				if (family != null) {
					String familyName = family.elementText("name");
					String cryptotechnique = family
							.elementText("cryptotechnique");
					String formatsize = family.elementText("formatsize");

					Family f = new Family(familyName,
							switchCryptoType(cryptotechnique),
							formatSizeIntegerValue(formatsize));

					this.tableSchema.addFamily(f);

					List<Element> qualifiersElement = family
							.elements("qualifier");
					for (Element qualifier : qualifiersElement) {
						String qualifierName = qualifier.elementText("name");
						String cryptotechniqueQualifier = qualifier
								.elementText("cryptotechnique");
						String qualifierFormatsize = qualifier
								.elementText("formatsize");

						Qualifier q = new Qualifier(qualifierName,
								switchCryptoType(cryptotechniqueQualifier),
								formatSizeIntegerValue(qualifierFormatsize));

						this.tableSchema.addQualifier(familyName, q);
					}
				}
			}
		}
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

}

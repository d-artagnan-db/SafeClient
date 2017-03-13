package pt.uminho.haslab.safecloudclient.schema;

import org.dom4j.*;
import org.dom4j.io.SAXReader;

import java.io.File;
import java.util.List;

/**
 * Created by rgmacedo on 3/13/17.
 */
public class SchemaParser {
    public TableSchema tableSchema;

    public SchemaParser() {
        this.tableSchema = new TableSchema();
    }

    public void parse(String filename) {
        try {
            long starttime = System.currentTimeMillis();

            File inputFile = new File(filename);
            SAXReader reader = new SAXReader();
            Document document = reader.read(inputFile);

            System.out.println("Root Element: "+document.getRootElement().getName());

            Element rootElement = document.getRootElement();

            parseTablename(rootElement);
            parseDefault(rootElement);
            parseKey(rootElement);
            parseColumns(rootElement);

            long stopttime = System.currentTimeMillis();
            System.out.println("Parsing Time: "+(stopttime-starttime));
        } catch (DocumentException e) {
            e.printStackTrace();
        }
    }

    public void parseTablename(Element rootElement) {
        Element nameElement = rootElement.element("name");
        String name = nameElement.getText();
        if(name != null) {
            this.tableSchema.setTablename(name);
        }

        System.out.println("ParseTablename - Name: "+name);
    }

    public void parseDefault(Element rootElement) {
        Element defaultElement = rootElement.element("default");
        if(defaultElement != null) {
            String key = defaultElement.elementText("key");
            String columns = defaultElement.elementText("columns");
            String formatsize = defaultElement.elementText("formatsize");

            if(key!=null)
                System.out.println("ParseDefault - Key: "+key);
            if(columns != null)
                System.out.println("ParseDefault - Columns: "+columns);
            if(formatsize != null)
                System.out.println("ParseDefault - FormatSize: "+formatsize);
        }
    }

    public void parseKey(Element rootElement) {
        Element keyElement = rootElement.element("key");
        if(keyElement != null) {
            String formatsize = keyElement.elementText("formatsize");
            String cryptotechnique = keyElement.elementText("cryptotechnique");

            if(formatsize != null)
                System.out.println("ParseKey - FormatSize: "+formatsize);
            if(cryptotechnique != null)
                System.out.println("ParseKey - CryptoTechnique: "+cryptotechnique);
        }
    }

    public void parseColumns(Element rootElement) {
        Element columnsElement = rootElement.element("columns");
        if(columnsElement != null) {
            List<Element> familiesElement = columnsElement.elements("family");
            for (Element family : familiesElement) {
                if(family != null) {
                    String familyName = family.elementText("name");
                    String cryptotechnique = family.elementText("cryptotechnique");
                    String formatsize = family.elementText("formatsize");

                    if(familyName != null)
                        System.out.println("FamilyName: "+familyName);
                    if(cryptotechnique != null)
                        System.out.println("CryptoTechnique: "+cryptotechnique);
                    if(formatsize != null)
                        System.out.println("FormatSize: "+formatsize);

                    List<Element> qualifiersElement = family.elements("qualifier");
                    for(Element qualifier : qualifiersElement) {
                        String qualifierName = qualifier.elementText("name");
                        String cryptotechniqueQualifier = qualifier.elementText("cryptotechnique");
                        String qualifierFormatsize = qualifier.elementText("formatsize");

                        if(qualifierName != null)
                            System.out.println("QualifierName: "+qualifierName);
                        if(cryptotechniqueQualifier != null)
                            System.out.println("QualifierCryptoTechnique: "+cryptotechniqueQualifier);
                        if(qualifierFormatsize != null)
                            System.out.println("QualifierFormatSize: "+qualifierFormatsize);

                    }
                }
            }
        }
    }

}

package uk.co.devworx.impala;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

/**
 * A utility that can be used to extract all the functions
 * from the Impala XML file
 */
public class FunctionExtractUtil
{
	static final DocumentBuilderFactory docBuilderFactory;
	static final DocumentBuilder docBuilder;
	static final XPathFactory xPathFactory;

	static
	{
		try
		{
			docBuilderFactory = DocumentBuilderFactory.newInstance();
			docBuilderFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, ""); // prevent external loading.
			docBuilderFactory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
			docBuilder = docBuilderFactory.newDocumentBuilder();
			xPathFactory = XPathFactory.newInstance();
		}
		catch(Exception e)
		{
			throw new RuntimeException("Unable to create an XML document factory. This is most unexpected : " + e);
		}
	}

	/**
	 * Imports all the functions from the given impala_functions.xml file.
	 *
	 * @param impalaFunctionsXml
	 * @return
	 */
	public static Set<String> extractFunctions(final Path impalaFunctionsXml) throws IOException
	{
		final Set<String> funcs = new TreeSet<>();
		try(InputStream reader = Files.newInputStream(impalaFunctionsXml))
		{
			Document doc = docBuilder.parse(reader);
			NodeList els = doc.getElementsByTagName("xref");
			for (int i = 0; i < els.getLength(); i++)
			{
				Element candidate = (Element)els.item(i);
				String content = candidate.getTextContent();
				if(content == null) content = "";
				content = content.trim();
				if(content.equals("")) continue;

				String href = candidate.getAttribute("href");
				if(href.contains("functions.xml") == false)
				{
					continue;
				}
				content = content.toLowerCase();
				if(content.contains("- analytic"))
				{
					content = content.substring(0, content.indexOf("- analytic")).trim();
				}

				String[] split = content.split(",");
				for (int j = 0; j < split.length; j++)
				{
					if(split[j].trim().equals("") == false)
					{
						funcs.add(split[j].trim());
					}
				}

			}

		} catch (SAXException e)
		{
			throw new RuntimeException("Could not parse the file - encountered : " + e, e);
		}

		return funcs;

	}


}

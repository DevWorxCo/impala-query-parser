package uk.co.devworx.impala;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;

/**
 * Test case for extracting the functions.
 *
 */
public class FunctionExtractUtilTest
{
	static final Logger logger = LogManager.getLogger(FunctionExtractUtilTest.class);

	@Test
	public void testExtractFunctionItems() throws Exception
	{
		Set<String> allFuncs = FunctionExtractUtil.extractFunctions(Paths.get("src/main/resources/impala-functions/impala_functions.xml"));
		Assert.assertNotNull(allFuncs);
		Assert.assertTrue(allFuncs.size() > 0 );

		logger.info("All Functions ");
		for(String func : allFuncs)
		{
			logger.info(func);
		}

		StringBuilder listItem = new StringBuilder();

		listItem.append("java.util.Arrays.asList(");
		for(String func : allFuncs)
		{
			listItem.append("\"" + func + "\",\n");
		}
		listItem.deleteCharAt(listItem.length() - 2);
		listItem.append(");");

		logger.info("\n\n" + listItem);

	}


}

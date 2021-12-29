package uk.co.devworx.impala;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

public class StatementFileFactoryTest
{
	static final Logger logger = LogManager.getLogger(StatementFileFactoryTest.class);

	@Test
	public void testSubstituteOutNotCommandMarkers() throws Exception
	{
		final String input = "'\n"
				+ "The stock reference data table\\'s definition that also contains some\n"
				+ "semi-colons (;) and other escaped quote (\") type items, that may confuse the parser splitter.\n"
				+ "'"
				+ "\n"
				+ "STORED AS PARQUET tblproperties (\"parquet.compression\"=\"SNAPPY\");\n"
				+ "\n"
				+ "compute incremental stats ${DATABASE}.stock_reference_data;\n"
				+ "";

		final String outputValue = StatementFileFactory.substituteOutNotCommandMarkers(input);

		logger.info("Output Value : " + outputValue);

		Assert.assertEquals(84, outputValue.indexOf(StatementFileFactory.CMD_NOT_AN_END_MARKER_SUBSTITUTE_STR));
	}


}

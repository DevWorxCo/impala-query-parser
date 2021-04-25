package uk.co.devworx.impala;

import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.StatementBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * Attempt to use the Apache Impala Front-End Library
 */
public class ParserTest
{
	@Test
	public void testFrontEndParsing() throws Exception
	{
		final StatementBase parserBase = Parser.parse("SELECT 1");
		Assert.assertNotNull(parserBase);
	}

}

package uk.co.devworx.impala;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Testing out the string segment parsing.
 */
public class StringSegmentParseTest
{
	static final Logger logger = LogManager.getLogger(StringSegmentParseTest.class);

	@Test
	public void test01() throws Exception
	{
		List<StringSegment> segments = testStringParsing("Some Content ; Yet more -- Some comments", "test01");
		Assert.assertNotNull(segments);
	}

	@Test
	public void test02() throws Exception
	{
		List<StringSegment> segments = testStringParsing("Some Content \"Quoted Items\" ; Some More Plain", "test02");
		Assert.assertNotNull(segments);
		Assert.assertTrue(StringSegment.hasEndOfCommandMarker(segments));
	}

	@Test
	public void test03() throws Exception
	{
		List<StringSegment> segments = testStringParsing("Some Content \"Quoted Items\" -- ; Some More Plain", "test03");
		Assert.assertNotNull(segments);
		Assert.assertFalse(StringSegment.hasEndOfCommandMarker(segments));
	}

	@Test
	public void test04() throws Exception
	{
		List<StringSegment> segments = testStringParsing("Some Content \"Quoted ; Items\" And some more Text at the end -- With Comment ;", "test04");
		Assert.assertNotNull(segments);
		Assert.assertFalse(StringSegment.hasEndOfCommandMarker(segments));
	}

	@Test
	public void test05() throws Exception
	{
		List<StringSegment> segments = testStringParsing("Some Content \"Quoted \\\"; Items\" And some more Text at the end -- With Comment ;", "test05");
		Assert.assertNotNull(segments);
		Assert.assertFalse(StringSegment.hasEndOfCommandMarker(segments));
	}

	private List<StringSegment> testStringParsing(String input, String testName)
	{
		logger.info("-----------------------------------------------------------------------------------\n" +
					"Test Name : " + testName + "\n" +
					"-----------------------------------------------------------------------------------\n" +
					input + "\n" +
					"-----------------------------------------------------------------------------------\n"
		 		  );
		List<StringSegment> sgms = StatementFileFactory.parseSegments(input);
		for (StringSegment sgm : sgms)
		{
			logger.info(sgm);
		}

		char[] chars = input.toCharArray();
		for (int i = 0; i < chars.length; i++)
		{
			logger.info(i + " -> " + chars[i]);
		}

		logger.info("-----------------------------------------------------------------------------------");

		return sgms;
	}

}

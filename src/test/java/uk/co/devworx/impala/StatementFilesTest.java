package uk.co.devworx.impala;

import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.StatementBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Testing the parsing of the statement files.
 */
public class StatementFilesTest
{
	static final Logger logger = LogManager.getLogger(StatementFilesTest.class);

	public static final Path INPUT_DIR = Paths.get("src/test/resources/test-sql");
	public static final Path INPUT_DIR_BROKEN = Paths.get("src/test/resources/test-sql-broken");

	public static final Path WORKING_DIR = Paths.get("target/StatementFilesTest");

	@Test
	public void testParsingOfStatementFiles() throws Exception
	{
		final StatementFiles stmtFiles = StatementFiles.create(INPUT_DIR, WORKING_DIR, new VariableReplacer());
		final List<StatementFile> stmts = stmtFiles.getStatementFiles();

		Assert.assertEquals(3,stmts.size());

		int successes = 0;
		int failures = 0;

		for (StatementFile stmt : stmts)
		{
			List<StatementFileParsed> filesParsed = stmt.getFilesParsed();
			for (StatementFileParsed sfp : filesParsed)
			{
				if(sfp.isSuccessful()) successes++;
				else failures++;
			}
		}

		logger.info("Successfully Parsed : " + successes);
		logger.info("Failed Parsing : " + failures);

		Assert.assertEquals(0,failures);
		Assert.assertEquals(14,successes);

	}

	@Test
	public void testParsingOfStatementFilesBroken() throws Exception
	{
		final StatementFiles stmtFiles = StatementFiles.create(INPUT_DIR_BROKEN, WORKING_DIR);
		final List<StatementFile> stmts = stmtFiles.getStatementFiles();

		Assert.assertEquals(3,stmts.size());

		int successes = 0;
		int failures = 0;

		for (StatementFile stmt : stmts)
		{
			List<StatementFileParsed> filesParsed = stmt.getFilesParsed();
			for (StatementFileParsed sfp : filesParsed)
			{
				if(sfp.isSuccessful()) successes++;
				else
				{
					logger.info(sfp.getFailureSummary());
					failures++;
				}
			}
		}

		logger.info("Successfully Parsed : " + successes);
		logger.info("Failed Parsing : " + failures);

		Assert.assertEquals(3,failures);
		Assert.assertEquals(11,successes);

	}

}

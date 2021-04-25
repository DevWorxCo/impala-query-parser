package uk.co.devworx.impala;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.*;

/**
 * Testing of path relativisms
 */
public class PathRelativismTest
{
	static final Logger logger = LogManager.getLogger(PathRelativismTest.class);

	@Test
	public void testPathRelatives() throws Exception
	{
		final Path workDir = Paths.get("target/PathRelativismTest");
		if(Files.exists(workDir) == false)
		{
			Files.createDirectories(workDir);
		}

		final Path rootDir = Paths.get("src/test/resources/test-paths").toAbsolutePath();
		final Path workFile = rootDir.resolve("p1/p2/placeholder.txt").toAbsolutePath();

		logger.info("Root Dir : " + rootDir);
		logger.info("Work File : " + workFile);

		Path relvf = rootDir.relativize(workFile);

		logger.info("Relativised Path : " + relvf);

		Path finalResolved = workDir.resolve(relvf);

		logger.info("finalResolved : " + finalResolved.toAbsolutePath());

		//Creating the parent
		Files.createDirectories(finalResolved.getParent());

		Files.copy(workFile, finalResolved, StandardCopyOption.REPLACE_EXISTING);

		Path newPath = Paths.get("target/PathRelativismTest/p1/p2/placeholder.txt");

		Assert.assertTrue(Files.exists(newPath));
	}

}

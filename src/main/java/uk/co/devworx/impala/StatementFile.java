package uk.co.devworx.impala;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents an Impala file - with sub file into which it has been split.
 *
 */
public class StatementFile
{
	static final Logger logger = LogManager.getLogger(StatementFile.class);

	public static final String CMD_END_MARKER = ";";
	public static final String COMMENT_MARKER = "--";

	/**
	 * Creates a statements file
	 *
	 */
	public static StatementFile create(	final Path sqlFileP,
										final Path rootDirectoryP,
										final Path workingDirectoryRootP) throws ImpalaQueryException
	{
		final Path sqlFileRelative = rootDirectoryP.toAbsolutePath().relativize(sqlFileP.toAbsolutePath());
		final Path sqlFileTarget = workingDirectoryRootP.resolve(sqlFileRelative);

		deleteAndRecreateTargetDirectory(sqlFileTarget);

		//Ok, now we can create the sub paths in this folder for each of the logical commands.

		final AtomicInteger origLineCounter = new AtomicInteger(1);
		final AtomicInteger newLineCounter = new AtomicInteger(1);
		final DecimalFormat dmcf = new DecimalFormat("0000");
		final AtomicInteger fileCounter = new AtomicInteger(1);

		Path lastTargetFile = sqlFileTarget.resolve(dmcf.format(fileCounter.get()) + ".sql");
		BufferedWriter targetOut = null;

		try(final BufferedReader inSQL = Files.newBufferedReader(sqlFileP))
		{
			targetOut = Files.newBufferedWriter(lastTargetFile);
			String line = null;
			while((line = inSQL.readLine()) != null)
			{
				origLineCounter.incrementAndGet();

				//Ok, now check if this line constitutes the end of a command.
				boolean isEndOfCommand = containsEndOfCommandMarker(line);


			}

		}
		catch(IOException ioe)
		{
			String msg = "Unable to read the input SQL file : " + sqlFileP + " - got the exception : " + ioe;
			logger.error(msg, ioe);
		}


		//Create the output directory
		//sqlFile.relativize(rootDirectory)

		//BufferedReader sqlIn = Files.newBufferedReader(sqlFile);

		

		return null;
	}

	/**
	 * Checks if this line indicates an end of line marker
	 * @param line
	 * @return
	 */
	static boolean containsEndOfCommandMarker(final String line)
	{
		String lineTrimmed = line.trim();
		if(lineTrimmed.endsWith(CMD_END_MARKER))
		{
			return true;
		}
		if(lineTrimmed.equals("")) return false;

		//Ok, determine if the command marker exists at all
		if(lineTrimmed.contains(CMD_END_MARKER) == false) return false;

		//See if the marker is commented out.
		int commentIndex = lineTrimmed.indexOf(COMMENT_MARKER);
		if(commentIndex != -1)
		{
			String lineExcludingComment = line.substring(0, commentIndex);
			return containsEndOfCommandMarker(lineExcludingComment);
		}

		//Ok, no comments - but it contains a ';' - so lets see if it is inside a quote

		return false;

	}

	/**
	 * Deletes and recreates the directory.
	 * @param sqlFileTarget
	 */
	private static void deleteAndRecreateTargetDirectory(Path sqlFileTarget)
	{
		try
		{
			if(Files.exists(sqlFileTarget) == true && Files.isDirectory(sqlFileTarget) == false)
			{
				Files.delete(sqlFileTarget);
				Files.createDirectories(sqlFileTarget);
				return;
			}
			if(Files.exists(sqlFileTarget) == true && Files.isDirectory(sqlFileTarget) == true)
			{
				Files.walk(sqlFileTarget)
						.sorted(Comparator.reverseOrder())
						.map(Path::toFile)
						.forEach(File::delete);
				Files.createDirectories(sqlFileTarget);
				return;
			}
			Files.createDirectories(sqlFileTarget);
		}
		catch(IOException e)
		{
			throw new ImpalaQueryException("Unable to delete and recreate the file/directory - " + sqlFileTarget + " - got the exception : " + e);
		}


	}

	private final Path sqlFile;
	private final List<Path> commandFiles;
	private final Map<Integer, TargetLex> linesLex;

	private StatementFile(Path sqlFile)
	{
		this.sqlFile = sqlFile;
		this.commandFiles = new ArrayList<>();
		this.linesLex = new TreeMap<>();
	}
}

class EndOfCommandMarker
{
	//public static final EndOfCommandMarker FALSE = n

	/*
	private final boolean isMarker;
	private final Optional<String> contentBefore;
	private final Optional<String> contentAfter;
	*/
}


class TargetLex
{
	public static final TargetLex BLANK = new TargetLex(-1, null);

	private final int lineNumber;
	private final Path targetFile;

	TargetLex(int lineNumber, Path targetFile)
	{
		this.lineNumber = lineNumber;
		this.targetFile = targetFile;
	}
}


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
 * A factory class for a Statement File.
 */
public class StatementFileFactory
{
	static final Logger logger = LogManager.getLogger(StatementFileFactory.class);

	public static final String CMD_END_MARKER = ";";
	public static final String COMMENT_MARKER = "--";

	public static final String DOUBLE_QUOTE_MARKER = "\"";
	public static final String SINGLE_QUOTE_MARKER = "'";
	public static final String DOUBLE_QUOTE_ESCAPE = "\\\"";
	public static final String SINGLE_QUOTE_ESCAPE = "\\'";

	private StatementFileFactory() {}

	/**
	 * Creates a statements file
	 *
	 */
	public static StatementFile create(	   final StatementFiles parent,
										   final Path sqlInputFileP,
										   final Path rootDirectoryP,
										   final Path workingDirectoryRootP,
										   final Optional<StatementFilePreProcessor> filePreProcessorP) throws ImpalaQueryException
	{
		final Path sqlFileRelative = rootDirectoryP.toAbsolutePath().relativize(sqlInputFileP.toAbsolutePath());
		final Path sqlFileTarget = workingDirectoryRootP.resolve(sqlFileRelative);

		deleteAndRecreateTargetDirectory(sqlFileTarget);

		//Ok, now we can create the sub paths in this folder for each of the logical commands.

		final AtomicInteger origLineCounter = new AtomicInteger(0);
		final AtomicInteger newLineCounter = new AtomicInteger(0);

		final DecimalFormat dmcf = new DecimalFormat("0000");
		final AtomicInteger fileCounter = new AtomicInteger(0);

		Path lastTargetFile = sqlFileTarget.resolve(dmcf.format(fileCounter.incrementAndGet()) + ".sql");
		BufferedWriter targetOut = null;

		final SortedMap<Integer, TargetLex> linesLex = new TreeMap<>();
		final ArrayList<Path> commandFiles = new ArrayList<>();

		commandFiles.add(lastTargetFile);

		//filePreProcessorP

		final Path processedSqlFile = getProcessedSQLFile(sqlInputFileP, filePreProcessorP, sqlFileTarget);

		try(final BufferedReader inSQL = Files.newBufferedReader(processedSqlFile))
		{
			targetOut = Files.newBufferedWriter(lastTargetFile);
			String line = null;

			while((line = inSQL.readLine()) != null)
			{
				final int origLineNumber = origLineCounter.incrementAndGet();

				final EndOfCommandMarker eocMarker = parseEndOfCommandMarker(line, origLineNumber, sqlInputFileP);
				if(eocMarker.isMarker == false)
				{
					targetOut.write(line);
					targetOut.newLine();

					int newLineNumber = newLineCounter.incrementAndGet();
					linesLex.put(origLineNumber, new TargetLex(newLineNumber, lastTargetFile));

					continue;
				}
				else
				{
					//We have to write the pre-content appropriately, close the file and start a new one.

					if(eocMarker.contentBefore.isPresent() && eocMarker.contentAfter.isPresent() )
					{
						targetOut.write(eocMarker.contentBefore.get());
						targetOut.newLine();
						targetOut.close();

						int lastNewLine = newLineCounter.incrementAndGet();
						newLineCounter.set(0);
						int firstNewLine = newLineCounter.incrementAndGet();

						Path lastFileBefore = lastTargetFile;

						lastTargetFile = sqlFileTarget.resolve(dmcf.format(fileCounter.incrementAndGet()) + ".sql");
						commandFiles.add(lastTargetFile);
						targetOut = Files.newBufferedWriter(lastTargetFile);

						targetOut.write(eocMarker.contentAfter.get());
						targetOut.newLine();

						linesLex.put(origLineNumber, new TargetLex(lastNewLine, lastFileBefore, firstNewLine, lastTargetFile));
					}
					else if(eocMarker.contentBefore.isPresent() && !eocMarker.contentAfter.isPresent() )
					{
						targetOut.write(eocMarker.contentBefore.get());
						targetOut.newLine();
						targetOut.close();

						int lastNewLine = newLineCounter.incrementAndGet();
						newLineCounter.set(0);

						linesLex.put(origLineNumber, new TargetLex(lastNewLine, lastTargetFile));

						lastTargetFile = sqlFileTarget.resolve(dmcf.format(fileCounter.incrementAndGet()) + ".sql");
						commandFiles.add(lastTargetFile);
						targetOut = Files.newBufferedWriter(lastTargetFile);

					}
					else if(!eocMarker.contentBefore.isPresent() && eocMarker.contentAfter.isPresent() )
					{
						targetOut.close();

						newLineCounter.set(0);
						int firstNewLine = newLineCounter.incrementAndGet();

						lastTargetFile = sqlFileTarget.resolve(dmcf.format(fileCounter.incrementAndGet()) + ".sql");
						commandFiles.add(lastTargetFile);
						targetOut = Files.newBufferedWriter(lastTargetFile);

						targetOut.write(eocMarker.contentAfter.get());
						targetOut.newLine();

						linesLex.put(origLineNumber, new TargetLex(firstNewLine, lastTargetFile));
					}
					else if(!eocMarker.contentBefore.isPresent() && !eocMarker.contentAfter.isPresent() )
					{
						targetOut.close();
						lastTargetFile = sqlFileTarget.resolve(dmcf.format(fileCounter.incrementAndGet()) + ".sql");
						commandFiles.add(lastTargetFile);
						targetOut = Files.newBufferedWriter(lastTargetFile);
					}
					else
					{
						throw new RuntimeException("Unexpected Condition - two optionals must equate to one of these 4 conditions.");
					}
				}
			}
		}
		catch(IOException ioe)
		{
			String msg = "Unable to read the input SQL file : " + sqlInputFileP + " - got the exception : " + ioe;
			logger.error(msg, ioe);
			throw new ImpalaQueryException(msg, ioe);
		}

		return new StatementFile(parent, sqlInputFileP, commandFiles, linesLex);
	}

	private static Path getProcessedSQLFile(final Path sqlInputFile,
											final Optional<StatementFilePreProcessor> filePreProcessor,
											final Path sqlFileTargetDir)
	{
		if(filePreProcessor.isPresent() == false) return sqlInputFile;
		final StatementFilePreProcessor processor = filePreProcessor.get();
		final Path sqlInputFileProcessed = sqlFileTargetDir.resolve(sqlInputFile.getFileName() + ".processed");
		processor.preProcess(sqlInputFile, sqlInputFileProcessed);
		return sqlInputFileProcessed;
	}



	/**
	 * Creates the end of command marker for this line
	 * @param line
	 * @param lineNumber
	 * @param sqlFileP
	 * @return
	 */
	static EndOfCommandMarker parseEndOfCommandMarker(final String line, int lineNumber, Path sqlFileP) throws ImpalaQueryException
	{
		if(line.contains(CMD_END_MARKER) == false)
		{
			return EndOfCommandMarker.EMPTY;
		}

		final List<StringSegment> segments = parseSegments(line);
		if(StringSegment.hasEndOfCommandMarker(segments) == false)
		{
			return EndOfCommandMarker.EMPTY;
		}

		if(StringSegment.hasEndOfCommandMarker(segments) == false)
		{
			throw new ImpalaQueryException("Multiple End of Command Tokens (" + CMD_END_MARKER + ") found on line " + lineNumber + " of file " + sqlFileP + " - this is not a pattern that is recommend. You should split these into multiple lines for the parser to process appropriately.");
		}

		//Ok, now return the marker details.
		return StringSegment.createEndOfCommandMarker(segments);
	}

	static List<StringSegment> parseSegments(final String lineP)
	{
		final String line = lineP.replace(DOUBLE_QUOTE_ESCAPE, "X")
				.replace(SINGLE_QUOTE_MARKER, "Y"); //Make sure we don't have to deal with quotes.

		final List<StringSegment> segments = new ArrayList<>();
		int index = 0;
		StringSegmentType currentSegment = StringSegmentType.Plain;
		boolean closedQuote = false;

		while(index < line.length())
		{
			closedQuote = false;

			if(currentSegment.equals(StringSegmentType.CommentedOut))
			{
				segments.add(new StringSegment(lineP, index, lineP.length(), StringSegmentType.CommentedOut));
				break;
			}

			if(currentSegment.equals(StringSegmentType.EndOfCommandMarker))
			{
				segments.add(new StringSegment(lineP, index, index+1, StringSegmentType.EndOfCommandMarker));
				currentSegment = StringSegmentType.Plain;
				index++;
				continue;
			}

			final TreeMap<Integer, StringSegmentType> segRanked = new TreeMap<>();
			if(currentSegment.equals(StringSegmentType.DoubleQuoted))
			{
				int dblQuote = line.indexOf(DOUBLE_QUOTE_MARKER, index + 1);
				if (dblQuote == -1)  dblQuote = line.length();
				segRanked.put(dblQuote + 1, StringSegmentType.DoubleQuoted);
				closedQuote = true;
			}
			else if(currentSegment.equals(StringSegmentType.SingleQuoted))
			{
				int singleQuote = line.indexOf(SINGLE_QUOTE_MARKER, index + 1);
				if (singleQuote == -1)  singleQuote = line.length();
				segRanked.put(singleQuote + 1, StringSegmentType.SingleQuoted);
				closedQuote = true;
			}
			else
			{
				int dblQuote = line.indexOf(DOUBLE_QUOTE_MARKER, index);
				if (dblQuote == -1)  dblQuote = line.length();
				segRanked.put(dblQuote, StringSegmentType.DoubleQuoted);

				int singleQuote = line.indexOf(SINGLE_QUOTE_MARKER, index);
				if (singleQuote == -1)  singleQuote = line.length();
				segRanked.put(singleQuote, StringSegmentType.SingleQuoted);

				int commentMarker = line.indexOf(COMMENT_MARKER, index);
				if (commentMarker == -1) commentMarker = line.length();
				segRanked.put(commentMarker, StringSegmentType.CommentedOut);

				int commandEndMarker = line.indexOf(CMD_END_MARKER, index);
				if (commandEndMarker == -1)  commandEndMarker = line.length();
				segRanked.put(commandEndMarker, StringSegmentType.EndOfCommandMarker);
			}

			//Find the next max one.
			Map.Entry<Integer, StringSegmentType> nextRanked = segRanked.firstEntry();
			if (nextRanked.getKey() != index)  segments.add(new StringSegment(lineP, index, nextRanked.getKey(), currentSegment));
			index = nextRanked.getKey();
			currentSegment = nextRanked.getValue();
			if(closedQuote == true)
			{
				currentSegment = StringSegmentType.Plain;
			}
		}


		return segments;

	}

	private static int countEndOfCommandCharacters(final String line)
	{
		char[] chrs = line.toCharArray();
		int count = 0;
		for (int i = 0; i < chrs.length; i++)
		{
			if(chrs[i] == CMD_END_MARKER.charAt(0)) count++;
		}
		return count;
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

}


enum StringSegmentType
{
	Plain,
	DoubleQuoted,
	SingleQuoted,
	CommentedOut,
	EndOfCommandMarker
}

class StringSegment
{
	public final String originalLine;
	public final int startIndex;
	public final int endIndex;
	public final StringSegmentType type;

	StringSegment(String originalLine, int startIndex, int endIndex, StringSegmentType type)
	{
		this.originalLine = originalLine;
		this.startIndex = startIndex;
		this.endIndex = endIndex;
		this.type = type;
	}

	public static EndOfCommandMarker createEndOfCommandMarker(final List<StringSegment> segments)
	{
		StringBuilder before = new StringBuilder();
		StringBuilder after = new StringBuilder();
		boolean markerReached = false;
		for (StringSegment seg : segments)
		{
			if(seg.type.equals(StringSegmentType.EndOfCommandMarker))
			{
				markerReached = true;
				continue;
			}

			if(markerReached == false)
			{
				before.append(seg.getStringValue());
			}
			else
			{
				after.append(seg.getStringValue());
			}
		}

		return new EndOfCommandMarker(markerReached,
									  before.length() > 0 ? Optional.of(before.toString()) : Optional.empty(),
									  after.length() > 0 ? Optional.of(after.toString()) : Optional.empty()
		);

	}

	@Override public String toString()
	{
		return "StringSegment{startIndex=" + startIndex + ", endIndex=" + endIndex + ", type=" + type + ", StringValue=\"" + getStringValue() + "\"}'";
	}

	private String getStringValue()
	{
		return originalLine.substring(startIndex, endIndex);
	}

	public static boolean hasEndOfCommandMarker(List<StringSegment> segments)
	{
		return segments.stream().filter(s -> s.type.equals(StringSegmentType.EndOfCommandMarker)).findAny().isPresent();
	}

	public static boolean hasMultipleEndOfCommandMarker(List<StringSegment> segments)
	{
		return segments.stream().filter(s -> s.type.equals(StringSegmentType.EndOfCommandMarker)).count() > 1;
	}

}


class EndOfCommandMarker
{
	public static EndOfCommandMarker EMPTY = new EndOfCommandMarker(false, Optional.empty(), Optional.empty());

	public final boolean isMarker;
	public final Optional<String> contentBefore;
	public final Optional<String> contentAfter;

	EndOfCommandMarker(boolean isMarker,
					   Optional<String> contentBefore,
					   Optional<String> contentAfter)
	{
		this.isMarker = isMarker;
		this.contentBefore = contentBefore;
		this.contentAfter = contentAfter;
	}
}








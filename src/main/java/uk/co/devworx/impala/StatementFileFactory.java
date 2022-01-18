package uk.co.devworx.impala;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

	public static final char CMD_END_MARKER = ';';
	public static final String CMD_END_MARKER_STR = String.valueOf(CMD_END_MARKER);
	public static final char COMMENT_MARKER_CHAR = '-';
	public static final String COMMENT_MARKER = "" + COMMENT_MARKER_CHAR + COMMENT_MARKER_CHAR;

	public static final char NEW_LINE_CHAR = '\n';

	public static final String DOUBLE_QUOTE_MARKER = "\"";
	public static final char SINGLE_QUOTE_MARKER = '\'';
	public static final String SINGLE_QUOTE_MARKER_STR = String.valueOf(SINGLE_QUOTE_MARKER);
	public static final String DOUBLE_QUOTE_ESCAPE = "\\\"";
	public static final String SINGLE_QUOTE_ESCAPE = "\\'";

	static final char ESCAPED_QUOTE_SUBSTITUTE = '☺';
	static final String ESCAPED_QUOTE_SUBSTITUTE_STR = String.valueOf(ESCAPED_QUOTE_SUBSTITUTE);
	static final String ESCAPED_DOUBLE_QUOTE_SUBSTITUTE = "☹";
	static final char CMD_NOT_AN_END_MARKER_SUBSTITUTE = '✌';
	static final String CMD_NOT_AN_END_MARKER_SUBSTITUTE_STR = String.valueOf(CMD_NOT_AN_END_MARKER_SUBSTITUTE);

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
					targetOut.write(handleEOCReplace(line));
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
						targetOut.write(handleEOCReplace(eocMarker.contentBefore.get()));
						targetOut.newLine();
						targetOut.close();

						int lastNewLine = newLineCounter.incrementAndGet();
						newLineCounter.set(0);
						int firstNewLine = newLineCounter.incrementAndGet();

						Path lastFileBefore = lastTargetFile;

						lastTargetFile = sqlFileTarget.resolve(dmcf.format(fileCounter.incrementAndGet()) + ".sql");
						commandFiles.add(lastTargetFile);
						targetOut = Files.newBufferedWriter(lastTargetFile);

						targetOut.write(handleEOCReplace(eocMarker.contentAfter.get()));
						targetOut.newLine();

						linesLex.put(origLineNumber, new TargetLex(lastNewLine, lastFileBefore, firstNewLine, lastTargetFile));
					}
					else if(eocMarker.contentBefore.isPresent() && !eocMarker.contentAfter.isPresent() )
					{
						targetOut.write(handleEOCReplace(eocMarker.contentBefore.get()));
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

						targetOut.write(handleEOCReplace(eocMarker.contentAfter.get()));
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

	/**
	 * Take care of the substitute replacement (if applicable).
	 * @param line
	 * @return
	 */
	private static String handleEOCReplace(final String line)
	{
		if(line.contains(CMD_NOT_AN_END_MARKER_SUBSTITUTE_STR))
		{
			return line.replace(CMD_NOT_AN_END_MARKER_SUBSTITUTE_STR, CMD_END_MARKER_STR);
		}
		return line;
	}

	private static Path getProcessedSQLFile(final Path sqlInputFile,
											final Optional<StatementFilePreProcessor> filePreProcessor,
											final Path sqlFileTargetDir)
	{
		if(filePreProcessor.isPresent() == false) return substituteOutNotCommandMarkers(sqlInputFile, sqlFileTargetDir);
		final StatementFilePreProcessor processor = filePreProcessor.get();
		final Path sqlInputFileProcessed = sqlFileTargetDir.resolve(sqlInputFile.getFileName() + ".processed");
		processor.preProcess(sqlInputFile, sqlInputFileProcessed);
		return substituteOutNotCommandMarkers(sqlInputFileProcessed, sqlFileTargetDir);
	}



	/**
	 * Make sure we susbstitute out all the markers that are contained within quotes that are not escaped.
	 * @param sqlInputFile
	 * @param sqlFileTargetDir
	 * @return
	 */
	static Path substituteOutNotCommandMarkers(final Path sqlInputFile,
											   final Path sqlFileTargetDir)
	{
		try
		{
			final Path sqlInputFileSubstituted = sqlFileTargetDir.resolve(sqlInputFile.getFileName() + ".substituted");
			String sqlFile = new String(Files.readAllBytes(sqlInputFile), StandardCharsets.UTF_8);
			String sqlFileSubbed = substituteOutNotCommandMarkers(sqlFile);

			Files.write(sqlInputFileSubstituted, sqlFileSubbed.getBytes(StandardCharsets.UTF_8));
			return sqlInputFileSubstituted;
		}
		catch(IOException ioe)
		{
			throw new RuntimeException("Unable to read and create substitutes for the SQL Input File : " + sqlInputFile + " | Encountered : " + ioe, ioe);
		}
	}

	static String substituteOutNotCommandMarkers(final String inputSQL)
	{
		class QuotedSegment
		{
			public final int quoteStart;
			public final int quoteEnd;

			private QuotedSegment(int quoteStart, int quoteEnd)
			{
				this.quoteStart = quoteStart;
				this.quoteEnd = quoteEnd;
			}

			@Override public String toString()
			{
				return "QuotedSegment{" + "quoteStart=" + quoteStart + ", quoteEnd=" + quoteEnd + '}';
			}

			public boolean isContainedWithin(int index)
			{
				return index > quoteStart && index < quoteEnd;
			}
		}

		String sqlFilePost = inputSQL.replace(SINGLE_QUOTE_ESCAPE, ESCAPED_QUOTE_SUBSTITUTE_STR);
		final StringBuilder sqlFilePostBuf = new StringBuilder(sqlFilePost);

		final List<QuotedSegment> quotedSegments = new ArrayList<>();
		final List<Integer> commandEndLocations = new ArrayList<>();

		//Now parse the segments.

		int quoteStart = -1;
		int quoteEnd = -1;

		boolean insideComment = false;

		for (int i = 0; i < sqlFilePost.length(); i++)
		{
			final char ch = sqlFilePost.charAt(i);
			final char chNext = (sqlFilePost.length() > (i+1) ) ? sqlFilePost.charAt(i+1) : ' ';

			if(quoteStart == -1 && ch == COMMENT_MARKER_CHAR && chNext == COMMENT_MARKER_CHAR)
			{
				insideComment = true;
			}

			if(ch == NEW_LINE_CHAR && quoteStart == -1)
			{
				insideComment = false;
			}

			if(ch == CMD_END_MARKER && insideComment == false)
			{
				commandEndLocations.add(i);
				continue;
			}

			if(insideComment == false && ch == SINGLE_QUOTE_MARKER && quoteStart == -1)
			{
				quoteStart = i;
				continue;
			}

			if(ch == SINGLE_QUOTE_MARKER && quoteStart != -1)
			{
				quoteEnd = i;
				insideComment = false;
				quotedSegments.add(new QuotedSegment(quoteStart, quoteEnd));
				quoteStart = -1;
				quoteEnd = -1;
				continue;
			}
		}

		if(quoteStart != -1) //Finish it off
		{
			quotedSegments.add(new QuotedSegment(quoteStart, sqlFilePost.length()-1));
		}

		//Ok, now replace all the command markers that are inside quotes.
		for(Integer index : commandEndLocations)
		{
			for(QuotedSegment segment : quotedSegments)
			{
				if(segment.isContainedWithin(index))
				{
					sqlFilePostBuf.setCharAt(index, CMD_NOT_AN_END_MARKER_SUBSTITUTE);
				}
			}
		}

		String sqlFilePostBuffer = sqlFilePostBuf.toString().replace(ESCAPED_QUOTE_SUBSTITUTE_STR, SINGLE_QUOTE_ESCAPE);
		return sqlFilePostBuffer;

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
		if(line.contains(CMD_END_MARKER_STR) == false)
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
		final String line = lineP.replace(DOUBLE_QUOTE_ESCAPE, ESCAPED_DOUBLE_QUOTE_SUBSTITUTE)
								 .replace(SINGLE_QUOTE_MARKER_STR, ESCAPED_QUOTE_SUBSTITUTE_STR); //Make sure we don't have to deal with quotes.

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
			if(chrs[i] == CMD_END_MARKER) count++;
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








package uk.co.devworx.impala;

import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.StatementBase;
import org.apache.impala.common.AnalysisException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Represents an Impala file - with sub file into which it has been split.
 *
 * You should obtain instances of this class by using the StatementFileFactory
 *
 */
public class StatementFile
{
	private final StatementFiles parent;
	private final Path sqlFile;

	private final List<Path> commandFiles;
	private final List<StatementFileParsed> parseds;

	private final SortedMap<Integer, TargetLex> linesLex;

	protected StatementFile(final StatementFiles parent,
							final Path sqlFile,
							final List<Path> commandFiles,
							final SortedMap<Integer, TargetLex> linesLex) throws ImpalaQueryException
	{
		this.parent = parent;
		this.sqlFile = sqlFile;
		this.commandFiles = Collections.unmodifiableList(commandFiles);
		this.linesLex = Collections.unmodifiableSortedMap(linesLex);
		parseds = commandFiles.stream().map(this::parsed).collect(Collectors.toList());
	}

	public OptionalInt findOriginalFileNumber(Path commandFile, int subNumber)
	{
		for (Map.Entry<Integer, TargetLex> e : linesLex.entrySet())
		{
			final Integer lineNumber = e.getKey();
			final TargetLex targetLex = e.getValue();

			if(targetLex.targetFile.equals(commandFile) && subNumber == targetLex.lineNumber)
			{
				return OptionalInt.of(lineNumber);
			}

			if(targetLex.secondaryTargetFile.isPresent())
			{
				if(targetLex.secondaryTargetFile.get().equals(commandFile) && subNumber == targetLex.secondaryLineNumber.getAsInt())
				{
					return OptionalInt.of(lineNumber);
				}
			}
		}
		return OptionalInt.empty();
	}

	public List<StatementFileParsed> getFilesParsed()
	{
		return parseds;
	}

	private StatementFileParsed parsed(final Path commandFile)
	{
		try
		{
			final String sqlContent = new String(Files.readAllBytes(commandFile));
			if(sqlContent.trim().equals(""))
			{
				return new StatementFileParsed(true, this, commandFile, Optional.empty(), Optional.empty(), Optional.empty());
			}

			try
			{
				StatementBase stmtBase = Parser.parse(sqlContent);
				return new StatementFileParsed(false, this, commandFile, Optional.empty(), Optional.of(stmtBase), Optional.empty());
			}
			catch(AnalysisException anle)
			{
				String msg = anle.getMessage();
				return new StatementFileParsed(false, this, commandFile, Optional.empty(), Optional.empty(), Optional.of(anle));
			}
		}
		catch(Exception e)
		{
			return new StatementFileParsed(false,this, commandFile, Optional.of(e), Optional.empty(), Optional.empty());
		}

	}

	public Path getSqlFile()
	{
		return sqlFile;
	}

	public List<Path> getCommandFiles()
	{
		return commandFiles;
	}

	public SortedMap<Integer, TargetLex> getLinesLex()
	{
		return linesLex;
	}

	@Override public boolean equals(Object o)
	{
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		StatementFile that = (StatementFile) o;
		return Objects.equals(sqlFile, that.sqlFile);
	}

	@Override public int hashCode()
	{
		return Objects.hash(sqlFile);
	}

	public StatementFiles getParent()
	{
		return parent;
	}


}


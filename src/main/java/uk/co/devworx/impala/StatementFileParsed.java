package uk.co.devworx.impala;

import org.apache.impala.analysis.StatementBase;
import org.apache.impala.common.AnalysisException;

import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Describing the parsed state of a specific file.
 *
 */
public class StatementFileParsed
{
	private static final String MARKER = "Syntax error in line";

	private final StatementFile parent;

	private final Path commandFile;

	private final boolean isBlank;

	private final Optional<Exception> generalError;
	private final Optional<StatementBase> statement;

	private final Optional<AnalysisException> parsingException;

	StatementFileParsed(boolean isBlank, StatementFile parent,
						Path commandFile,
						Optional<Exception> generalError,
						Optional<StatementBase> statement,
						Optional<AnalysisException> parsingException)
	{
		this.parent = parent;
		this.isBlank = isBlank;
		this.commandFile = commandFile;
		this.generalError = generalError;
		this.statement = statement;
		this.parsingException = parsingException;
	}

	/**
	 * Returns the line where the item has failed.
	 * @return
	 */
	public OptionalInt getCommandFileLineNumber()
	{
		if(parsingException.isPresent() == false) return OptionalInt.empty();

		String msg = parsingException.get().toString();
		int lineMarker = msg.indexOf(MARKER);
		if(lineMarker == -1) return OptionalInt.empty();

		String remainder = msg.substring(lineMarker + MARKER.length());
		int end = remainder.indexOf(":");
		if(end == -1) end = remainder.indexOf("\n");
		if(end == -1) end = remainder.length();

		String lineNumber = remainder.substring(0, end).trim();
		try
		{
			return OptionalInt.of(Integer.parseInt(lineNumber));
		}
		catch(Exception e)
		{
			return OptionalInt.empty();
		}
	}

	/**
	 * Returns the line number in the file where it has orginally failed.
	 * @return
	 */
	public OptionalInt getOriginalFileLineNumber()
	{
		final OptionalInt cmdFileNumber = getCommandFileLineNumber();
		if(cmdFileNumber.isPresent() == false) return OptionalInt.empty();
		int subNumber = cmdFileNumber.getAsInt();
		return parent.findOriginalFileNumber(commandFile, subNumber);
	}

	public String getFailureSummary()
	{
		if(isSuccessful() == true) return "SUCCESS";
		final StringBuilder report = new StringBuilder();

		report.append("\n-------------------------------------------------------------------------------------------------------------------\n");
		report.append("SQL File : " + parent.getSqlFile().toAbsolutePath());
		report.append("\n");
		OptionalInt origLineNumber = getOriginalFileLineNumber();
		report.append("SQL File Line Number : " + (origLineNumber.isPresent() ? "" + origLineNumber.getAsInt() : "n/a") );
		report.append("\n");
		report.append("Command SQL File : " + commandFile.toAbsolutePath());
		report.append("\n");
		OptionalInt commandFileNumber = getCommandFileLineNumber();
		report.append("Command SQL File Line Number : " + (commandFileNumber.isPresent() ? "" + commandFileNumber.getAsInt() : "n/a") );
		report.append("\n");
		report.append("Parsing Exception:\n");
		report.append("\n");
		report.append("Error Type : " + (generalError.isPresent() ? "General Error - " + generalError.get() : "Parsing Error" ));
		report.append("\n");
		report.append("Parsing Exception:\n");
		if(parsingException.isPresent() == true)
		{
			AnalysisException prse = this.parsingException.get();
			report.append(prse);
		}
		report.append("\n");
		report.append("-------------------------------------------------------------------------------------------------------------------\n");

		return report.toString();
	}

	public boolean isSuccessful()
	{
		return isBlank == true || statement.isPresent();
	}

	public StatementFile getParent()
	{
		return parent;
	}

	public Path getCommandFile()
	{
		return commandFile;
	}

	public Optional<Exception> getGeneralError()
	{
		return generalError;
	}

	public Optional<StatementBase> getStatement()
	{
		return statement;
	}

	public Optional<AnalysisException> getParsingException()
	{
		return parsingException;
	}


}



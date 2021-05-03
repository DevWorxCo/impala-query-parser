package uk.co.devworx.impala;

import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * The lexical location mapping from the parent file
 * line indexes to the child line indexes.
 */
public class TargetLex
{
	public final int lineNumber;
	public final Path targetFile;

	public final OptionalInt secondaryLineNumber;
	public final Optional<Path> secondaryTargetFile;

	TargetLex(int lineNumber, Path targetFile)
	{
		this.lineNumber = lineNumber;
		this.targetFile = targetFile;
		secondaryLineNumber = OptionalInt.empty();
		secondaryTargetFile = Optional.empty();
	}

	TargetLex(int lineNumber, Path targetFile, int secondaryLineNameP, Path secondaryTargetFileP)
	{
		this.lineNumber = lineNumber;
		this.targetFile = targetFile;

		secondaryLineNumber = OptionalInt.of(secondaryLineNameP);
		secondaryTargetFile = Optional.of(secondaryTargetFileP);
	}

}
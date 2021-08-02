package uk.co.devworx.impala;

import java.nio.file.Path;

/**
 * In cases where a script needs to be pre-processed before being passed into the parser
 * an optional implementation of the StatementFilePreProcessor class can be passed in.
 *
 * Useful if for instance there are environment variables that need to be replaced.
 */
@FunctionalInterface
public interface StatementFilePreProcessor
{
	/**
	 * @param inputScript - the location to the input file to be pre-processed.
	 * @param outputScriptPath the location of the output file with the script post processed.
	 */
	void preProcess(Path inputScript, Path outputScriptPath);
}

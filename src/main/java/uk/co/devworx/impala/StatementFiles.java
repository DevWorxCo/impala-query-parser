package uk.co.devworx.impala;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Impala script files typically contain many statements which are delimited by ';'
 *
 * The Impala parsing infrastructure expects that commands are done individually. This division it seems
 * is done in various places (ImpalaJdbcClient - search for split(";") and impala_shell.py).
 *
 * Hence, in order for us to have a query parser and checker, we need to replicate this same logic.
 *
 * However, in the case of this library, we also make sure to preserve the original lexical location in order to make debugging easier.
 *
 */
public class StatementFiles
{
	static final Logger logger = LogManager.getLogger(StatementFiles.class);

	public static final String SQL_FILE_EXTENSION = ".sql";

	/**
	 * Scans the supplied file system and creates
	 * an instance of the StatementFiles object.
	 * @param rootDirectory
	 * @param workingDirectoryRoot
	 * @return
	 */
	public static StatementFiles create(final Path rootDirectory, final Path workingDirectoryRoot) throws ImpalaQueryException
	{
		return StatementFiles.create(rootDirectory, workingDirectoryRoot, null);
	}

	/**
	 * Scans the supplied file system and creates
	 * an instance of the StatementFiles object.
	 * @param rootDirectory
	 * @param workingDirectoryRoot
	 * @param filePreProcessor - an optional file preprocessor that can be passed in to handle file value replacements.
	 * @return
	 */
	public static StatementFiles create(final Path rootDirectory, final Path workingDirectoryRoot, final StatementFilePreProcessor filePreProcessor) throws ImpalaQueryException
	{
		Objects.requireNonNull(rootDirectory, "You cannot specify a null root directory");
		Objects.requireNonNull(workingDirectoryRoot, "You cannot specify a null workingDirectoryRoot directory");
		return new StatementFiles(rootDirectory, workingDirectoryRoot, Optional.ofNullable(filePreProcessor));
	}

	private final Path rootDirectory;

	private final Path workingDirectoryRoot;

	private final List<Path> allSQLFiles;

	private final List<StatementFile> statementFiles;

	private final Optional<StatementFilePreProcessor> filePreProcessor;

	private StatementFiles(Path rootDirectory, Path workingDirectoryRoot, Optional<StatementFilePreProcessor> filePreProcessor) throws ImpalaQueryException
	{
		Objects.requireNonNull(filePreProcessor, "filePreProcessor cannot be null");

		if(Files.isDirectory(rootDirectory) == false)
		{
			throw new ImpalaQueryException("The root directory you supplied - " + rootDirectory + " - is not a directory or does not exist" );
		}

		if(Files.exists(workingDirectoryRoot) == true && Files.isDirectory(workingDirectoryRoot) == false)
		{
			throw new ImpalaQueryException("The working directory you supplied - " + workingDirectoryRoot + " - exists and is not a directory." );
		}

		if(Files.exists(workingDirectoryRoot) == false)
		{
			try
			{
				logger.info("Creating the directory : " + workingDirectoryRoot);
				Files.createDirectories(workingDirectoryRoot);
			}
			catch (IOException e)
			{
				String msg = "Unable to create the directory : " + workingDirectoryRoot + " - got the exception : " + e;
				logger.error(msg, e);
				throw new ImpalaQueryException(msg, e);
			}
		}

		this.rootDirectory = rootDirectory;
		this.workingDirectoryRoot = workingDirectoryRoot;
		this.filePreProcessor = filePreProcessor;
		allSQLFiles = _buildAllSQLFilesList(rootDirectory);
		statementFiles = allSQLFiles.stream().map(sqlFile -> _buildStatementFile(sqlFile, rootDirectory, workingDirectoryRoot)).collect(Collectors.toList());
	}

	private StatementFile _buildStatementFile(Path sqlFile, Path rootDirectory, Path workingDirectoryRoot) throws ImpalaQueryException
	{
		return StatementFileFactory.create(this, sqlFile, rootDirectory, workingDirectoryRoot, filePreProcessor);
	}

	private static List<Path> _buildAllSQLFilesList(Path rootDirectory)
	{
		//Ok, now visit all the files and create the appropriate instance.
		final List<Path> sqlFiles = new ArrayList<>();
		try
		{
			Files.walkFileTree(rootDirectory, new SimpleFileVisitor<Path>()
			{
				@Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
				{
					String fileName = file.getFileName().toString();
					if(fileName.endsWith(SQL_FILE_EXTENSION))
					{
						sqlFiles.add(file);
					}
					return FileVisitResult.CONTINUE;
				}
			});
			return Collections.unmodifiableList(sqlFiles);
		}
		catch(IOException e)
		{
			String msg = "Unable to walk the root directory path : " + rootDirectory + " - encountered exception : " + e;
			logger.error(msg);
			throw new ImpalaQueryException(msg, e);
		}
	}

	public Path getRootDirectory()
	{
		return rootDirectory;
	}

	public Path getWorkingDirectoryRoot()
	{
		return workingDirectoryRoot;
	}

	public List<Path> getAllSQLFiles()
	{
		return allSQLFiles;
	}

	public List<StatementFile> getStatementFiles()
	{
		return statementFiles;
	}
}











































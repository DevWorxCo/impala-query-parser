package uk.co.devworx.impala;

import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.CreateTableStmt;
import org.apache.impala.analysis.StatementBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

import static uk.co.devworx.impala.StatementFilesTest.INPUT_DIR;
import static uk.co.devworx.impala.StatementFilesTest.WORKING_DIR;

/**
 * Testing the parsing of the statement files.
 */
public class StatementFilesCommentExtractTest
{
	static final Logger logger = LogManager.getLogger(StatementFilesCommentExtractTest.class);

	@Test
	public void testExtractComments() throws Exception
	{
		final StatementFiles stmtFiles = StatementFiles.create(INPUT_DIR, WORKING_DIR);
		final List<StatementFile> stmts = stmtFiles.getStatementFiles();

		for (StatementFile stmtFl : stmts)
		{
			List<StatementFileParsed> filesParsed = stmtFl.getFilesParsed();
			for (int i = 0; i < filesParsed.size(); i++)
			{
				StatementFileParsed sfp = filesParsed.get(i);
				Path cmdFile = stmtFl.getCommandFiles().get(i);

				Optional<StatementBase> stmtOpt = sfp.getStatement();
				if(stmtOpt.isPresent() == false) continue;

				final StatementBase baseStmt = stmtOpt.get();

				logger.info(baseStmt + " -> From : " + cmdFile.toAbsolutePath());

				if(baseStmt instanceof CreateTableStmt)
				{
					CreateTableStmt tableStmt = (CreateTableStmt)baseStmt;
					String tableComment = tableStmt.getComment();
					logger.info("Table Comment : " + tableComment);
					List<ColumnDef> colDefs = tableStmt.getColumnDefs();
					for(ColumnDef colDef : colDefs)
					{
						logger.info(" colDef : " + colDef.getColName() + " | " + colDef.getTypeDef());
						String comment = colDef.getComment();
						if(comment != null)
						{
							logger.info(" COMMENT :: " + comment);
						}
					}

				}

			}
		}
	}



}

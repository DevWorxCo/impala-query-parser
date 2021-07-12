package uk.co.devworx.impala;

import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.CreateViewStmt;
import org.apache.impala.analysis.Parser;
import org.apache.impala.analysis.StatementBase;
import org.junit.Assert;
import org.junit.Test;

import javax.persistence.Column;
import java.util.List;
import java.util.Optional;

public class CreateViewStmtUtilsTest
{
	private static String VIEW_COMMENT = "Overall View Comment Line1\\n\"\n"
			+ "			+ \"         Line 2\\n\"\n"
			+ "			+ \"         Line 3\\n\"\n"
			+ "			+ \"         Line 4";

	public static String VIEW_STATMENT = "CREATE VIEW some_view\n"
			+ "(\n"
			+ "    col_1     COMMENT 'Column 1 - Comment',\n"
			+ "    col_2     COMMENT 'Column 2 - Comment',\n"
			+ "    col_3     COMMENT 'Column 3 - Comment',\n"
			+ "    col_4     COMMENT 'Column 4 - Comment',\n"
			+ "    col_5     COMMENT 'Column 5 - Comment'\n"
			+ ")\n"
			+ "COMMENT '" + VIEW_COMMENT + "'"
			+ "\n"
			+ "AS\n"
			+ "    SELECT  'col_1' as col_1,\n"
			+ "            'col_2' as col_2,\n"
			+ "            'col_3' as col_3,\n"
			+ "            'col_4' as col_4,\n"
			+ "            'col_5' as col_5 \n"
			+ ";\n"
			+ "";

	@Test
	public void testViewCommentExtract() throws Exception
	{
		final StatementBase stmtBase = Parser.parse(VIEW_STATMENT);
		Assert.assertTrue(stmtBase instanceof CreateViewStmt);
		CreateViewStmt stmt = (CreateViewStmt) stmtBase;

		Optional<String> viewCommentsOpt = CreateViewStmtUtils.getViewComments(stmt);

		Assert.assertTrue(viewCommentsOpt.isPresent());

		String viewComments = viewCommentsOpt.get();

		System.out.println(viewComments);

		Assert.assertEquals(VIEW_COMMENT, viewComments);

	}

	@Test
	public void testViewColumnExtract() throws Exception
	{
		final StatementBase stmtBase = Parser.parse(VIEW_STATMENT);
		Assert.assertTrue(stmtBase instanceof CreateViewStmt);
		CreateViewStmt stmt = (CreateViewStmt) stmtBase;

		Optional<List<ColumnDef>> columnDefsOpt = CreateViewStmtUtils.getViewColumnDefs(stmt);

		Assert.assertTrue(columnDefsOpt.isPresent());

		List<ColumnDef> columnDefs = columnDefsOpt.get();

		Assert.assertEquals(5, columnDefs.size());

		for (int i = 0; i < columnDefs.size(); i++)
		{
			Assert.assertEquals("Column " + (i+1) + " - Comment", columnDefs.get(i).getComment());
		}

	}

}






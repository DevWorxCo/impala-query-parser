package uk.co.devworx.impala;

import org.apache.impala.analysis.ColumnDef;
import org.apache.impala.analysis.CreateOrAlterViewStmtBase;
import org.apache.impala.analysis.CreateViewStmt;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

/**
 * The impala org.apache.impala.analysis.CreateViewStmt class
 * is unusually restrictive - to the point where it does not expose the
 * comments for the columns and/or the view.
 *
 * This utility class uses reflection to get to the internals of this instance....
 * Perhaps not the best way to do this, but this is the only short term solution.
 */
public class CreateViewStmtUtils
{
	private CreateViewStmtUtils()  {}

	public static final String VIEW_COMMENTS_PROPERTY = "comment_";
	public static final String VIEW_COLUMN_COMMENT_DEFS_PROPERTY = "columnDefs_";

	/**
	 * Returns the comments at the view level (if any) for this particular statement.
	 *
	 * @param stmt
	 * @return
	 */
	public static Optional<String> getViewComments(CreateViewStmt stmt)
	{
		Objects.requireNonNull(stmt, "You cannot pass in a null statement");

		try
		{
			Field fld = getCreateOrAlterViewStmtBase_Field(VIEW_COMMENTS_PROPERTY);
			Object commentObj = fld.get(stmt);
			if (commentObj == null)
			{
				return Optional.empty();
			} else
			{
				return Optional.of(String.valueOf(commentObj));
			}
		}
		catch (IllegalAccessException e)
		{
			throw new RuntimeException("Unable to extract the field value : " + VIEW_COMMENTS_PROPERTY  + " from : " + stmt + " | got the exception : " + e, e);
		}
	}

	/**
	 * Returns the column definitions for this view (if any) for this particular statement.
	 *
	 * @param stmt
	 * @return
	 */
	public static Optional<List<ColumnDef>> getViewColumnDefs(CreateViewStmt stmt)
	{
		Objects.requireNonNull(stmt, "You cannot pass in a null statement");
		try
		{
			Field fld = getCreateOrAlterViewStmtBase_Field(VIEW_COLUMN_COMMENT_DEFS_PROPERTY);
			Object commentObj = fld.get(stmt);
			if (commentObj == null)
			{
				return Optional.empty();
			}
			else
			{
				List<ColumnDef> colDefs = (List<ColumnDef>)commentObj;
				return Optional.of(Collections.unmodifiableList(colDefs));
			}
		}
		catch (IllegalAccessException e)
		{
			throw new RuntimeException("Unable to extract the field value : " + VIEW_COMMENTS_PROPERTY  + " from : " + stmt + " | got the exception : " + e, e);
		}
	}

	private static Field getCreateOrAlterViewStmtBase_Field(String name)
	{
		Class<CreateOrAlterViewStmtBase> cls = CreateOrAlterViewStmtBase.class;
		Field[] dclrFields = cls.getDeclaredFields();
		for(Field fld : dclrFields)
		{
			if(fld.getName().equals(name))
			{
				fld.setAccessible(true);
				return fld;
			}
		}
		throw new RuntimeException("Unable to find the field name : " + name + " from the class type :" + cls.getName() + " - available fields include: " +
									Arrays.stream(dclrFields).map(f -> f.getName()).collect(Collectors.toList()));
	}


}

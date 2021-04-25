package uk.co.devworx.impala;

/**
 * An exception that can be generated from the parsing process.
 */
public class ImpalaQueryException extends RuntimeException
{
	public ImpalaQueryException()
	{
	}

	public ImpalaQueryException(String message)
	{
		super(message);
	}

	public ImpalaQueryException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public ImpalaQueryException(Throwable cause)
	{
		super(cause);
	}

	public ImpalaQueryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
	{
		super(message, cause, enableSuppression, writableStackTrace);
	}
}

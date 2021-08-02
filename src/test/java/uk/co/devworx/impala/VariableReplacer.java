package uk.co.devworx.impala;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A test variable replacer...
 *
 */
public class VariableReplacer implements StatementFilePreProcessor
{
	private static final Map<String, String> variables;
	static
	{
		variables = new ConcurrentHashMap<>();
		variables.put("${DATABASE}", "mydatabase");
	}

	@Override public void preProcess(Path inputScript, Path outputScriptPath)
	{
		try(BufferedReader br = Files.newBufferedReader(inputScript);
			BufferedWriter bw = Files.newBufferedWriter(outputScriptPath))
		{
			String line = null;
			while((line = br.readLine()) != null)
			{
				for(Map.Entry<String, String> e : variables.entrySet())
				{
					line = line.replace(e.getKey(), e.getValue());
				}
				bw.write(line);
				bw.newLine();
			}
		}
		catch(IOException e)
		{
			throw new RuntimeException("Unable to do the replacement from - " + inputScript + " to " + outputScriptPath + " - encountered exception : " + e, e);
		}
	}

}

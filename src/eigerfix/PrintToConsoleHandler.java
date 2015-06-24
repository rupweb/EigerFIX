package eigerfix;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

import com.lmax.disruptor.EventHandler;

/**
 * Print to console
 * Just to show the benefit of disruptor and mimic IO cost
 */
public class PrintToConsoleHandler implements EventHandler<FixEvent> 
{
    private final int FLUSH_AFTER_SIZE = 1024;
    private final byte[] NEWLINE = System.getProperty("line.separator").getBytes();

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final Writer writer;

    public PrintToConsoleHandler() throws IOException 
    {
    	System.out.println("In PrintToConsoleHandler()");
    	out.reset();
    	writer = new OutputStreamWriter(new FileOutputStream("output-log.txt"), "utf-8");
    }

    public void onEvent(FixEvent event, long sequence, boolean endOfBatch) throws IOException 
    {
    	System.out.println("In PrintToConsoleHandler.onEvent()");
        out.write(event.message.toString().getBytes());
        out.write(NEWLINE, 0, NEWLINE.length);
        if (endOfBatch || (out.size() > FLUSH_AFTER_SIZE)) 
        {
        	try 
        	{
        		writer.write(out.toString());
        	} 
        	catch (IOException e) 
        	{
        		System.out.println("failed to write: ");
        		System.out.println(e);
        		e.printStackTrace();
        		throw new RuntimeException(e);
        	}
            out.reset();
        }
    }
}


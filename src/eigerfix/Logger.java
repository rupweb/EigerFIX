package eigerfix;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.SocketException;

import com.lmax.disruptor.EventHandler;

public class Logger implements EventHandler<FixEvent> 
{
    private final int FLUSH_AFTER_SIZE = 8;
    private final byte[] NEWLINE = System.getProperty("line.separator").getBytes();

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final Writer writer;

    public Logger() throws IOException, SocketException 
    {
    	System.out.println("In Logger()");
    	
    	out.reset();
    	
    	writer = new OutputStreamWriter(new FileOutputStream("moonsfix.log"), "utf-8");
    }	
	
	@Override
	public void onEvent(FixEvent event, long sequence, boolean endOfBatch) throws Exception 
	{	
    	System.out.println(Utils.now() + "EVENT: logger " + event.message.toString());
		
		out.write(event.message.toString().getBytes());
        out.write(NEWLINE, 0, NEWLINE.length);
        

		writer.write(out.toString());
        
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

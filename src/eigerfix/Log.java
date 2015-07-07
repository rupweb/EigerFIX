package eigerfix;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.lmax.disruptor.EventHandler;

public class Log implements EventHandler<FixEvent> 
{   
	static private FileHandler fileTxt;
	static private SimpleFormatter formatterTxt;

	static private FileHandler fileHTML;
	static private Formatter formatterHTML;
	
	// Get the global log to configure it
	Logger log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

	public Log() // throws IOException 
	{	
		log.setLevel(Level.INFO);
		try 
		{
			fileTxt = new FileHandler("D:\\temp\\EigerFIXLog.txt");
			fileHTML = new FileHandler("D:\\temp\\EigerFIXLog.html");
		} 
		catch (IOException e)
		{
			e.printStackTrace();
		}

		// Create text Formatter
		formatterTxt = new SimpleFormatter();
		fileTxt.setFormatter(formatterTxt);
		log.addHandler(fileTxt);

		// Create HTML Formatter
		formatterHTML = new LogHtml();
		fileHTML.setFormatter(formatterHTML);
		log.addHandler(fileHTML);
	}
	
	@Override
	public void onEvent(FixEvent event, long sequence, boolean endOfBatch) throws Exception 
	{	
    	// System.out.println(Utils.now() + "EVENT: log " + event.message.toString());
		
		log.info(event.message.toString().replace("", "  "));
	}
}

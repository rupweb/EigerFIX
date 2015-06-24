package eigerfix;

import quickfix.Message;
import quickfix.SessionID;

/**
 * Value event
 * This is the value event we will use to pass data between threads
 */
public class FixEvent  
{
    protected Message message;
    protected SessionID session;
    
	public void set(Message m, SessionID s) 
	{
    	System.out.println(Utils.now() + "In FixEvent.set() with message: " + m.toString());
		this.message = m;
		this.session = s;
	}
}

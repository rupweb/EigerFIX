package eigerfix;

import quickfix.FieldNotFound;
import quickfix.Initiator;
import quickfix.IntField;
import quickfix.Message;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.field.SenderCompID;
import quickfix.field.TargetCompID;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * The business logic function to send FIX messages from initiator to provider(s)
 */
public class DisruptorToProvidersLogic implements EventHandler<FixEvent> 
{
    final RingBuffer<FixEvent> ringbuffer;
    final Translator translator;
    final Initiator i;
	Session _session = null;

    public DisruptorToProvidersLogic(Disruptor<FixEvent> output, Initiator i) 
    {
    	System.out.println("In DisruptorToProvidersLogic()");
    	
    	// Initiator is used to do stuff
    	this.i = i;
    	
        // translator will be used to write events into the buffer
        this.translator = new Translator();
        
        // get a hold of the ringbuffer, we can't publish direct to Disruptor as the DSL doesn't
        // provide a garbage-free two-arg publishEvent method
        this.ringbuffer = output.getRingBuffer();
    }

    /// process events
    public void onEvent(FixEvent event, long sequence, boolean endOfBatch) throws FieldNotFound
    {
    	// What this needs to do is get the event to the initiator...
    	System.out.println(Utils.now() + "EVENT: DisruptorToProviders " + event.message.toString() + ", sequence: " + sequence + ", endOfBatch: " + endOfBatch);
    	
    	quickfix.Message m = event.message;
    		
		// Setup the header to send to the correct provider  	
    	SenderCompID oldSenderCompID = new SenderCompID();
    	m.getHeader().getField(oldSenderCompID);
    	String beginString = "FIX.4.4";
    	
    	switch (oldSenderCompID.getValue())
    	{
    		case "demo417_md":
    			m.getHeader().setField(new TargetCompID("GTXDEMO"));
    			break;
    			
    		default:
    			m.getHeader().setField(new SenderCompID("ipes.apiuat_ORD"));
    			m.getHeader().setField(new TargetCompID("GSL_FX"));
    	}	    	
    	
    	SenderCompID senderCompID = new SenderCompID();
    	TargetCompID targetCompID = new TargetCompID();
    	
    	String sender = m.getHeader().getField(senderCompID).getValue();
    	String target = m.getHeader().getField(targetCompID).getValue();
    	
        // Set the correct session for the initiator to send out to
    	SessionID s = new SessionID(beginString, sender, target);
    	
        // Don't just send on all message types
        
		String type = event.message.getClass().getSimpleName();
			
	    switch (type)
	    {
	        case "quickfix.fix44.Heartbeat":
	        	// Heartbeat, do nothing
	        	System.out.println(Utils.now() + "HEARTBEAT: do nothing");
	        	break;        		
	        		
	        case "quickfix.fix44.Logout":
	        	// Heartbeat, do nothing
	        	System.out.println(Utils.now() + "LOGOUT: do nothing");
	        	break;
	        		
	        case "quickfix.fix44.Logon":
	        	// Logon, do nothing
	        	System.out.println(Utils.now() + "LOGON: do nothing");
	        	break;
	        			
	        default:
	        	FIXSend(m, s, oldSenderCompID.getValue());
	            break;		        
	    }
    			
    	System.out.println("Out DisruptorToProvidersLogic.onEvent()");   	
    }

	public void FIXSend(Message m, SessionID s, String client)
	{
    	// Setup the relevant session
        _session = Session.lookupSession(s);
        
        // Record the FIX expected sequence number against client because for some FIX messages (35=3) which can come from
        // a provider, there is no reference given. All we then know is the sequence number of the previous message 
        int seq = _session.getExpectedSenderNum();
    	RfqCache.set_fix_client(seq, client);
        
		// Send the FIX message
	    try
	    {
	        if (_session != null)
	        {
	        	// Add tag 369 LastMsgSeqNumProcessed
	        	m.getHeader().setField(new IntField(369, _session.getExpectedSenderNum() - 1));
	            
	        	// Logging.LogInfo("SESSION: {0}, INP: {1}", _session.SessionID.ToString(), fm.toString());
	            System.out.println(Utils.now() + "SEND: provider " + m.toString());
	            _session.send(m);
	        }
	        else
	        {
	            // Logging.LogInfo("Can't send message: FIX session not created.");
	            // Logging.LogInfo(fm.toString());
	            System.out.println(Utils.now() + "Can't send message: FIX session not created.");
	            System.out.println(Utils.now() + " " + m.toString());
	        }
	    }
	    catch (Exception e)
	    {
			System.out.println(Utils.now() + "ERROR: FIXSend" );
	    	e.printStackTrace();
	    }
    }
}

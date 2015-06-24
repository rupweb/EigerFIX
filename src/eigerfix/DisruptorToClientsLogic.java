package eigerfix;

import quickfix.Acceptor;
import quickfix.FieldNotFound;
import quickfix.Message;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.field.SenderCompID;
import quickfix.field.TargetCompID;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * The business logic function to send FIX messages from acceptor to all acceptor clients
 */
public class DisruptorToClientsLogic implements EventHandler<FixEvent> 
{
    final RingBuffer<FixEvent> ringbuffer;
    final Translator translator;
    final Acceptor a;
	Session _session = null;

    public DisruptorToClientsLogic(Disruptor<FixEvent> output, Acceptor a) 
    {
    	System.out.println("In DisruptorToClientsLogic()");
    	this.a = a;
    	
        // translator will be used to write events into the buffer
        this.translator = new Translator();
        
        // get a hold of the ringbuffer, we can't publish direct to Disruptor as the DSL doesn't
        // provide a garbage-free two-arg publishEvent method
        this.ringbuffer = output.getRingBuffer();
    }

    /// process events
    public void onEvent(FixEvent event, long sequence, boolean endOfBatch) throws FieldNotFound 
    {
    	System.out.println(Utils.now() + "EVENT: DisruptorToClients " + event.message.toString() + ", sequence: " + sequence + ", endOfBatch: " + endOfBatch);
    	System.out.println("acceptor: " + a.getSessions().get(0).toString());
    	
    	quickfix.Message m = event.message;
    	SessionID s = event.session;
    	
    	// Replace the SenderCompID and TargetCompID
    	m.getHeader().setField(new SenderCompID("EIGER_FX"));
    	m.getHeader().setField(new TargetCompID(s.getSenderCompID()));
    	
        // Set the correct session for the acceptor to send out to   	
    	SessionID id = new SessionID("FIX.4.4", "EIGER_FX", s.getSenderCompID());
    	
        _session = Session.lookupSession(id);

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
	            FIXSend(m);
	            break;		        
	    }
		
    	System.out.println("Out DisruptorToClientsLogic.onEvent()");
    }

	public void FIXSend(Message fm)
	{
	    // Send the FIX message
	    try
	    {
	        if (_session != null)
	        {
	            // Logging.LogInfo("SESSION: {0}, INP: {1}", _session.SessionID.ToString(), fm.toString());
	            System.out.println(Utils.now() + "SEND: client " + fm.toString());
	            _session.send(fm);
	        }
	        else
	        {
	            // Logging.LogInfo("Can't send message: FIX session not created.");
	            // Logging.LogInfo(fm.toString());
	            System.out.println(Utils.now() + "Can't send message: FIX session not created.");
	            System.out.println(Utils.now() + " " + fm.toString());
	        }
	    }
	    catch (Exception e)
	    {
			System.out.println(Utils.now() + "ERROR: FIXSend" );
	    	e.printStackTrace();
	    }
    		
        // then publish direct to buffer
        // final byte[] toSendBytes = toUpperCase(event.buffer, event.length);
        // ringbuffer.publishEvent(translator, toSendBytes, event.address);
    	
    	// ringbuffer.publishEvent(translator);
    }
}

package eigerfix;

import quickfix.FieldNotFound;
import quickfix.IncorrectTagValue;
import quickfix.Initiator;
import quickfix.Message;
import quickfix.MessageCracker;
import quickfix.SessionID;
import quickfix.UnsupportedMessageType;
import quickfix.field.ClOrdID;
import quickfix.field.QuoteReqID;
import quickfix.field.RefSeqNum;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * The business logic function to send FIX messages from acceptor to all acceptor clients
 */
public class DisruptorFromProvidersLogic extends MessageCracker implements EventHandler<FixEvent> 
{
    final RingBuffer<FixEvent> ringbuffer;
    final Translator translator;
    final Initiator i;
	private DisruptorToClients d;

    public DisruptorFromProvidersLogic(Disruptor<FixEvent> output, Initiator i, DisruptorToClients d) throws Exception 
    {
    	System.out.println("In DisruptorFromProvidersLogic()");
    	this.i = i;
    	
        // translator will be used to write events into the buffer
        this.translator = new Translator();
        
        // get a hold of the ringbuffer, we can't publish direct to Disruptor as the DSL doesn't
        // provide a garbage-free two-arg publishEvent method
        this.ringbuffer = output.getRingBuffer();
        
        this.d = d;
    }

    /// process events
    public void onEvent(FixEvent event, long sequence, boolean endOfBatch) throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue 
    {
    	System.out.println(Utils.now() + "EVENT: DisruptorFromProviders " + event.message.toString() + ", sequence: " + sequence + ", endOfBatch: " + endOfBatch);
    	
    	quickfix.Message m = event.message;
    	SessionID s = event.session;
    	
    	System.out.println("provider initiator: " + s.toString());    	
    	
    	try
    	{
    		crack(m, s);
    	}
    	catch (Exception e)
    	{
			System.out.println(Utils.now() + "ERROR: No message cracker" );
	    	e.printStackTrace();
    	}
		
    	System.out.println("Out DisruptorFromProvidersLogic.onEvent()");
    }
    
    public void onMessage(quickfix.fix44.Quote m, SessionID s) throws FieldNotFound
    { 
        // Get the client to send this quote back to   	
    	String request = m.getField(new QuoteReqID()).getValue();
    	String client = RfqCache.get_rfq_client(request);
    	 	
        // Replace the session id with the new client
    	SessionID session = new SessionID(s.getBeginString(), s.getTargetCompID(), client);
    	
    	// Pass off to be sent    	
    	PublishForSending(m, session);		        
	}
    
    public void onMessage(quickfix.fix44.QuoteCancel m, SessionID s) throws FieldNotFound
    { 
        // Get the client to send this quote back to   	
    	String request = m.getField(new QuoteReqID()).getValue();
    	String client = RfqCache.get_rfq_client(request);
    	 	
        // Replace the session id with the new client
    	SessionID session = new SessionID(s.getBeginString(), s.getTargetCompID(), client);
    	
    	// Pass off to be sent    	
    	PublishForSending(m, session);		        
	}    
    
    public void onMessage(quickfix.fix44.QuoteResponse m, SessionID s) throws FieldNotFound
    { 
        // Get the client to send this quote back to   	
    	String request = m.getField(new QuoteReqID()).getValue();
    	String client = RfqCache.get_rfq_client(request);
    	 	
        // Replace the session id with the new client
    	SessionID session = new SessionID(s.getBeginString(), s.getTargetCompID(), client);
    	
    	// Pass off to be sent    	
    	PublishForSending(m, session);		        
	}
    
    public void onMessage(quickfix.fix44.QuoteRequestReject m, SessionID s) throws FieldNotFound
    { 
        // Get the client to send this quote back to   	
    	String request = m.getField(new QuoteReqID()).getValue();
    	String client = RfqCache.get_rfq_client(request);
    	 	
        // Replace the session id with the new client
    	SessionID session = new SessionID(s.getBeginString(), s.getTargetCompID(), client);
    	
    	// Pass off to be sent    	
    	PublishForSending(m, session);		        
	}
    
    public void onMessage(quickfix.fix44.MarketDataIncrementalRefresh m, SessionID s) throws FieldNotFound
    { 
        // In a market data message there's no quote ID. Get the client from the expected sequence number
    	RefSeqNum seq = new RefSeqNum();
    	String client = RfqCache.get_fix_client(m.getHeader().getField(seq).getValue());
    	 	
        // Replace the session id with the new client
    	SessionID session = new SessionID(s.getBeginString(), s.getTargetCompID(), client);
    	
    	// Pass off to be sent    	
    	PublishForSending(m, session);		        
	}    
    
    public void onMessage(quickfix.fix44.MarketDataSnapshotFullRefresh m, SessionID s) throws FieldNotFound
    { 
        // In a market data message there's no quote ID. Get the client from the sequence number
    	RefSeqNum seq = new RefSeqNum();
    	String client = RfqCache.get_fix_client(m.getHeader().getField(seq).getValue());
    	
        // Replace the session id with the new client
    	SessionID session = new SessionID(s.getBeginString(), s.getTargetCompID(), client);
    	
    	// Pass off to be sent    	
    	PublishForSending(m, session);		        
	}    
    
    public void onMessage(quickfix.fix44.Reject m, SessionID s) throws FieldNotFound
    { 
        // In a reject message there's no quote ID, order ID or reference at all. Get the client from the sequence number
    	RefSeqNum seq = new RefSeqNum();

    	String client = RfqCache.get_fix_client(m.get(seq).getValue());
   	 	
        // Replace the session id with the new client
    	SessionID session = new SessionID(s.getBeginString(), s.getTargetCompID(), client);
    	
    	// Pass off to be sent    	
    	PublishForSending(m, session);		        
	}   
    
    public void onMessage(quickfix.fix44.BusinessMessageReject m, SessionID s) throws FieldNotFound
    { 
        // In a reject message there's no quote ID, order ID or reference at all. Get the client from the sequence number
    	RefSeqNum seq = new RefSeqNum();

    	String client = RfqCache.get_fix_client(m.get(seq).getValue());
   	 	
        // Replace the session id with the new client
    	SessionID session = new SessionID(s.getBeginString(), s.getTargetCompID(), client);
    	
    	// Pass off to be sent    	
    	PublishForSending(m, session);		        
	}      
    
    public void onMessage(quickfix.fix44.ExecutionReport m, SessionID s) throws FieldNotFound
    { 
        // Get the client to send this quote back to   	
    	String request = m.getField(new ClOrdID()).getValue();
    	String client = RfqCache.get_order_client(request);
    	 	
        // Replace the session id with the new client
    	SessionID session = new SessionID(s.getBeginString(), s.getTargetCompID(), client);
    	
    	// Pass off to be sent    	
    	PublishForSending(m, session);		        
	}    

	public void PublishForSending(Message m, SessionID s)
	{
		// Publish the FIX message
	    try
	    {
	    	d.Publish(m, s);
	    }
	    catch (Exception e)
	    {
			System.out.println(Utils.now() + "ERROR: DisruptorFromProviders publishing error" );
	    	e.printStackTrace();
	    }
    		
        // then publish direct to buffer
        // final byte[] toSendBytes = toUpperCase(event.buffer, event.length);
        // ringbuffer.publishEvent(translator, toSendBytes, event.address);
    	
    	// ringbuffer.publishEvent(translator);
    }
}

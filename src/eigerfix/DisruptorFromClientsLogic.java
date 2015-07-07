package eigerfix;

import quickfix.Acceptor;
import quickfix.FieldNotFound;
import quickfix.IncorrectTagValue;
import quickfix.Message;
import quickfix.MessageCracker;
import quickfix.SessionID;
import quickfix.StringField;
import quickfix.UnsupportedMessageType;
import quickfix.field.ClOrdID;
import quickfix.field.QuoteReqID;
import quickfix.field.SenderCompID;
import quickfix.fix44.MarketDataRequest;
import quickfix.fix44.NewOrderSingle;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * The business logic to handle
 * 		- Price Requests
 * 		- Deal Requests
 * and then to pass off the messages to the DisruptorToProviders for sending
 */
public class DisruptorFromClientsLogic extends MessageCracker implements EventHandler<FixEvent> 
{
    final RingBuffer<FixEvent> ringbuffer;
    final Translator translator;
    final Acceptor a;
	private DisruptorToProviders d;

    public DisruptorFromClientsLogic(Disruptor<FixEvent> output, Acceptor a, DisruptorToProviders d) throws Exception 
    {
    	System.out.println("In DisruptorFromClientsLogic()");
    	
    	// Acceptor is used to identify sessions
    	this.a = a;
    	
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
    	// What this needs to do is get the event to the initiator...
    	System.out.println(Utils.now() + "EVENT: DisruptorFromClients " + event.message.toString() + ", sequence: " + sequence + ", endOfBatch: " + endOfBatch);
    	
    	quickfix.Message m = event.message;
    	SessionID s = event.session;
    	
    	System.out.println("client initiator: " + s.toString());
    	   	
    	/* Set the correct initiator session depending on who sent the message to the acceptor
    	String eventSource = m.getHeader().getField(new StringField(49)).getValue();
   	
    	for (int j = 0; j < a.getSessions().size(); j++)
    	{
        	String eventDestination = a.getSessions().get(j).getTargetCompID();
    		if (eventSource.equals(eventDestination))
    			s = a.getSessions().get(j);
    	}
    	*/
    	
    	// Crack the message event using quickfix message cracker to account for differences in data dictionaries
    	try
    	{
    		crack(m, s);
    	}
    	catch (Exception e)
    	{
			System.out.println(Utils.now() + "ERROR: No message cracker" );
	    	e.printStackTrace();
    	}
    			
    	System.out.println("Out DisruptorFromClientsLogic.onEvent()");   	
    }
    
    public void onMessage(quickfix.fix43.QuoteRequest m, SessionID s) throws FieldNotFound 
    { 
    	String request = m.getField(new QuoteReqID()).getValue();
    	String client = m.getHeader().getField(new SenderCompID()).getValue();
    	
    	RfqCache.set_rfq_client(request, client);
    	
    	m = Blender.RFQ43(m);
    	
    	// Pass off to be sent
    	PublishForSending(m, s);		        
	}
    
    public void onMessage(quickfix.fix44.QuoteRequest m1, SessionID s) throws FieldNotFound
    { 
		// If there is only 1 provider and we don't want them to know who our clients are, then:
		// Save the reference and id in a name value pair lookup table
		// The lookup table is then read by the FromProvidersLogic to set the correct client again
		
		String request = m1.get(new QuoteReqID()).getValue();
		String client = m1.getHeader().getField(new SenderCompID()).getValue();
		
		RfqCache.set_rfq_client(request, client);
    	
    	m1 = Blender.RFQ44(m1);
    	MarketDataRequest m2 = Blender.RFQtoMD(m1);
    	
		// GainGTX sender comp id
		SenderCompID senderCompID = new SenderCompID("demo417_md");
		m2.getHeader().setField(senderCompID);
    	
    	// Pass off to be sent    	
    	PublishForSending(m1, s);
    	PublishForSending(m2, s);   	
	}
    
    public void onMessage(NewOrderSingle m, SessionID s) throws FieldNotFound
    { 
        // If there is only 1 provider and we don't want them to know who our clients are, then:
    	// Save the reference and id in a name value pair lookup table
    	// The lookup table is then read by the FromProvidersLogic to set the correct client again
    	
    	String order = m.get(new ClOrdID()).getValue();
    	String client = m.getHeader().getField(new SenderCompID()).getValue();
    	
    	RfqCache.set_order_client(order, client);
    	
    	// Change tag 303 from 2 to 102
    	m.setField(new StringField(303, "2"));
    	
    	// Pass off to be sent    	
    	PublishForSending(m, s);		        
	}    
    
    public void onMessage(quickfix.fix44.Reject m, SessionID s)
    { 
    	// rejects from an initiator are sent to the FIX monitor. That's already handled by the disruptor log event... right?
	}

    // TODO: add all the other initiator error message handling that's required.
    
	public void PublishForSending(Message m, SessionID s)
	{
		// Publish the FIX message
	    try
	    {
	    	d.Publish(m, s);
	    }
	    catch (Exception e)
	    {
			System.out.println(Utils.now() + "ERROR: DisruptorFromClients publishing error" );
	    	e.printStackTrace();
	    }
    		
        // then publish direct to buffer
        // final byte[] toSendBytes = toUpperCase(event.buffer, event.length);
        // ringbuffer.publishEvent(translator, toSendBytes, event.address);
    	
    	// ringbuffer.publishEvent(translator);
    }
}

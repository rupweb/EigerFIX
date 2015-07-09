package eigerfix;

import quickfix.FieldNotFound;
import quickfix.Group;
import quickfix.IncorrectTagValue;
import quickfix.Initiator;
import quickfix.Message;
import quickfix.MessageCracker;
import quickfix.SessionID;
import quickfix.UnsupportedMessageType;
import quickfix.field.BidPx;
import quickfix.field.BidSpotRate;
import quickfix.field.BusinessRejectRefID;
import quickfix.field.ClOrdID;
import quickfix.field.MDEntryPx;
import quickfix.field.MDEntryType;
import quickfix.field.MDReqID;
import quickfix.field.OfferPx;
import quickfix.field.OfferSpotRate;
import quickfix.field.QuoteID;
import quickfix.field.QuoteReqID;
import quickfix.field.RefSeqNum;
import quickfix.field.Symbol;
import quickfix.fix44.MarketDataSnapshotFullRefresh.NoMDEntries;

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
    
    /* public void onMessage(quickfix.fix44.Quote m, SessionID s) throws FieldNotFound
    { 
        // Get the client to send this quote back to   	
    	String request = m.getField(new QuoteReqID()).getValue();
    	String client = RfqCache.get_rfq_client(request);
    	 	
        // Replace the session id with the new client
    	SessionID session = new SessionID(s.getBeginString(), s.getTargetCompID(), client);
    	
    	// Pass off to be sent    	
    	PublishForSending(m, session);		        
	} */
    
    public void onMessage(quickfix.fix44.Quote m, SessionID s) throws FieldNotFound
    { 
    	// Publishing quotes to the ToClient disruptor for sending to all clients is handled by the pricer
    	Price p = new Price();
    	p.base_currency = m.getField(new Symbol()).getValue().substring(0, 3);
    	p.terms_currency = m.getField(new Symbol()).getValue().substring(4, 7);    	
    	p.rfq_id = m.getField(new QuoteReqID()).getValue();
    	p.quote_id = m.getField(new QuoteID()).getValue();
    	p.max_size = 1000000;
    	p.bid = m.getField(new BidPx()).getValue();
    	p.offer = m.getField(new OfferPx()).getValue();
    	p.bid_spot_rate = m.getField(new BidSpotRate()).getValue();
    	p.offer_spot_rate = m.getField(new OfferSpotRate()).getValue();   	
    	// Get the value date for this quote request     	   	
    	p.value_date = RfqCache.get_rfq_value_date(p.rfq_id);
    	
    	Pricer.setPrice(p, d);
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
        // With market data there's no connection to any client. Update the price array
    	Price p = new Price();
    	p.base_currency = m.getField(new Symbol()).getValue().substring(0, 3);
    	p.terms_currency = m.getField(new Symbol()).getValue().substring(4, 7);    	
    	p.rfq_id = m.getField(new MDReqID()).getValue();
    	p.quote_id = RfqCache.get_rfq_quote(p.rfq_id);
    	p.max_size = 1000000;
    	// Get the value date for this quote request     	   	
    	p.value_date = RfqCache.get_rfq_value_date(p.rfq_id);
    	
    	int numberOfMarketDataEntries = m.getNoMDEntries().getValue();
  	  
    	for (int i = 1; i <= numberOfMarketDataEntries; i++)
    	{
    		Group group = m.getGroup(i, new NoMDEntries());

    		if (group.getField(new MDEntryType()).valueEquals('0'))
    		{
    			p.bid = group.getField(new MDEntryPx()).getValue();
    			p.bid_spot_rate = group.getField(new MDEntryPx()).getValue();
    		}
    		else if (group.getField(new MDEntryType()).valueEquals('1'))
    		{
    			p.offer = group.getField(new MDEntryPx()).getValue();
    			p.offer_spot_rate = group.getField(new MDEntryPx()).getValue();
    		}
    	}    	
    		
    	// Publishing to the ToClient disruptor for sending to all clients is handled by the pricer
    	Pricer.setPrice(p, d);
	}    
    
    public void onMessage(quickfix.fix44.MarketDataSnapshotFullRefresh m, SessionID s) throws FieldNotFound
    { 
        // With market data there's no connection to any client. Update the price array
    	Price p = new Price();
    	p.base_currency = m.getField(new Symbol()).getValue().substring(0, 3);
    	p.terms_currency = m.getField(new Symbol()).getValue().substring(4, 7);    	
    	p.rfq_id = m.getField(new MDReqID()).getValue();
    	p.quote_id = RfqCache.get_rfq_quote(p.rfq_id);
    	p.max_size = 1000000;
    	// Get the value date for this quote request     	   	
    	p.value_date = RfqCache.get_rfq_value_date(p.rfq_id);
    	   	
    	int numberOfMarketDataEntries = m.getNoMDEntries().getValue();
    	  
    	for (int i = 1; i <= numberOfMarketDataEntries; i++)
    	{
    		Group group = m.getGroup(i, new NoMDEntries());

    		if (group.getField(new MDEntryType()).valueEquals('0'))
    		{
    			p.bid = group.getField(new MDEntryPx()).getValue();
    			p.bid_spot_rate = group.getField(new MDEntryPx()).getValue();
    		}
    		else if (group.getField(new MDEntryType()).valueEquals('1'))
    		{
    			p.offer = group.getField(new MDEntryPx()).getValue();
    			p.offer_spot_rate = group.getField(new MDEntryPx()).getValue();
    		}
    	}
    	 	
    	// Publishing to the ToClient disruptor for sending to all clients is handled by the pricer
    	Pricer.setPrice(p, d);
	}    
    
	public void onMessage(quickfix.fix44.MarketDataRequestReject m, SessionID s) throws FieldNotFound 
	{		
    	String request = m.getField(new MDReqID()).getValue();
    	String client = RfqCache.get_rfq_client(request);
    	 	
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
    	String request = m.getField(new BusinessRejectRefID()).getValue();
    	String client = RfqCache.get_order_client(request);
   	 	
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

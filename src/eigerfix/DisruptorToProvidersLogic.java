package eigerfix;

import quickfix.FieldNotFound;
import quickfix.Group;
import quickfix.IncorrectTagValue;
import quickfix.Initiator;
import quickfix.IntField;
import quickfix.Message;
import quickfix.MessageCracker;
import quickfix.Session;
import quickfix.SessionID;
import quickfix.StringField;
import quickfix.UnsupportedMessageType;
import quickfix.field.Account;
import quickfix.field.Currency;
import quickfix.field.OrderQty;
import quickfix.field.QuoteReqID;
import quickfix.field.QuoteRequestType;
import quickfix.field.SenderCompID;
import quickfix.field.SettlDate;
import quickfix.field.SettlType;
import quickfix.field.Symbol;
import quickfix.field.TargetCompID;
import quickfix.fix44.QuoteRequest;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * The business logic function to send FIX messages from initiator to provider(s)
 */
public class DisruptorToProvidersLogic extends MessageCracker implements EventHandler<FixEvent> 
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
    public void onEvent(FixEvent event, long sequence, boolean endOfBatch) 
    		throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue 
    {
    	// What this needs to do is get the event to the initiator...
    	System.out.println(Utils.now() + "EVENT: DisruptorToProviders " + event.message.toString() + ", sequence: " + sequence + ", endOfBatch: " + endOfBatch);
    	System.out.println("initiator: " + i.getSessions().get(0).toString());
    	
        // Set the correct initiator session
    	SessionID id = i.getSessions().get(0);
    	
    	quickfix.Message m = event.message;
    	
    	// Crack the message event using quickfix message cracker to account for differences in data dictionaries
    	crack(m, id);
    			
    	System.out.println("Out DisruptorToProvidersLogic.onEvent()");   	
    }
    
    public void onMessage(QuoteRequest m, SessionID s)
    	      throws FieldNotFound, UnsupportedMessageType, IncorrectTagValue 
    { 
    	// Rebuild the Quote Request in a different tag order and grouping
    	
    	// Setup tag types
		QuoteReqID id = new QuoteReqID();
    	Symbol symbol = new Symbol();
    	OrderQty amount = new OrderQty();
    	Account account = new Account();
    	Currency ccy = new Currency();
    	SettlType sType = new SettlType();
    	SettlDate sDate = new SettlDate();
    	QuoteRequestType qt = new QuoteRequestType();
    	
    	// Populate non repeating tags
		id = (QuoteReqID) m.getField(id);
    			
		// Take group without using data dictionary
		Group g = m.getGroup(1, 146); 
		
		// There's a casting problem and you have to go through the hoops
		StringField symbol1 = g.getField(new StringField(55));
		IntField qt1 = g.getField(new IntField(303));
		StringField sType1 = g.getField(new StringField(63));		
		StringField sDate1 = g.getField(new StringField(64));
		StringField ccy1 = g.getField(new StringField(15));
		StringField account1 = g.getField(new StringField(1));		
		
		// refresh data		
    	symbol = new Symbol(symbol1.getValue());
    	qt = new QuoteRequestType(qt1.getValue());
		amount = (OrderQty) g.getField(amount);
		sType = new SettlType(sType1.getValue());		
		sDate = new SettlDate(sDate1.getValue());
		ccy = new Currency(ccy1.getValue());
		account = new Account(account1.getValue());

    	// Create Quote Request message
		QuoteRequest qr = new QuoteRequest(id);
		
		// Populate tags
		qr.setField(symbol);
		
		// Setup a group and specify an index for each group tag
		Group g2 = new Group(146, 1);
		g2.setField(1, qt);	
		g2.setField(2, amount);
		g2.setField(3, sType);
		g2.setField(4, sDate);
		g2.setField(5, ccy);
		g2.setField(6, account);
		
    	// add group to request
    	qr.addGroup(g2);
    	
		// Setup the header
    	qr.getHeader().setField(new SenderCompID(s.getSenderCompID()));   	
    	qr.getHeader().setField(new TargetCompID("GSL_FX"));
    	
	    FIXSend(qr, s);		        
	}

	public void FIXSend(Message m, SessionID s)
	{
    	// Setup the relevant session
        _session = Session.lookupSession(s);
		
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
    		
        // then publish direct to buffer
        // final byte[] toSendBytes = toUpperCase(event.buffer, event.length);
        // ringbuffer.publishEvent(translator, toSendBytes, event.address);
    	
    	// ringbuffer.publishEvent(translator);
    }
}

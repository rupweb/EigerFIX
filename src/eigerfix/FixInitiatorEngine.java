package eigerfix;

import quickfix.*;
import quickfix.field.MsgType;

public class FixInitiatorEngine extends MessageCracker implements Application
{
    // There's only 1 FIX session here, so global this to be used outside this class...
	public Session _market_data_session = null;
	public Session _trading_session = null;
	private DisruptorFromProviders d;
	
	// TODO: if a logout message is sent for a particular provider, just log out that provider

	// Constructor
	public FixInitiatorEngine(DisruptorFromProviders disruptorFromProviders) throws Exception
	{
        System.out.println("In FixInitiatorEngine");
        this.d = disruptorFromProviders;
	}
	
    public void fromApp(Message msg, SessionID s) throws UnsupportedMessageType, FieldNotFound, IncorrectTagValue
    {
        System.out.println(Utils.now() + "INITIATOR: FromApp "  + msg.getClass() + " " + msg.toString());
        // Logging.LogInfo("FROMAPP: {0}", msg.toString());

    	// Crack FIX message any way you want to
        crack(msg, s);
    }

    public void toApp(Message msg, SessionID s)
    {
        System.out.println(Utils.now() + "INITIATOR: ToApp " + msg.getClass() + " " + msg.toString());
        // Logging.LogInfo("TOAPP: {0}", msg.toString());

    	//fixout.Publish(msg.toString());         
    }

    public void fromAdmin(Message msg, SessionID s) throws FieldNotFound, IncorrectTagValue
    {
        System.out.println(Utils.now() + "INITIATOR: FromAdmin " + msg.getClass() + " " + msg.toString()); 
        
        // Send reject messages back to acceptor            
        if (msg.getHeader().getString(MsgType.FIELD).equals(MsgType.REJECT))
        {
            System.out.println(Utils.now() + "Reject from: " + s.toString());
            d.Publish(msg, s);
        }
        
        // Send reject messages back to acceptor            
        if (msg.getHeader().getString(MsgType.FIELD).equals(MsgType.QUOTE_REQUEST_REJECT))
        {
            System.out.println(Utils.now() + "Quote Request Reject from: " + s.toString());
            d.Publish(msg, s);
        }
    }

    public void onCreate(SessionID s) 
    {
        System.out.println(Utils.now() + "INITIATOR: OnCreate " + s.toString());
    }

    public void onLogout(SessionID s)
    {
        System.out.println(Utils.now() + "INITIATOR: OnLogout " + s.toString());
        //errorlog.Publish("INITIATOR:OnLogout, FIX does not respond, call FIX support");
    }

    public void onLogon(SessionID s)
    {
        System.out.println(Utils.now() + "INITIATOR: OnLogon " + s.toString());   			
    }

    public void toAdmin(Message msg, SessionID s)
    {
        System.out.println(Utils.now() + "INITIATOR: ToAdmin " + msg.getClass() + " " + msg.toString());
    	
    	// Don't send out unsupported message type messages to providers
        /*
        if (msg.Header.GetField(quickfix.Fields.Tags.MsgType) == MsgType.BUSINESS_MESSAGE_REJECT)
        {
        	System.out.println("ERRO ToAdmin BMR: " + msg.toString());
        	return;
        }
        
        // Don't send out reject messages to providers            
        if (msg.Header.GetField(quickfix.Fields.Tags.MsgType) == MsgType.REJECT)
        {
        	System.out.println("ERRO ToAdmin R: " + msg.toString());
        	return;
        }
        */
        
       	try 
       	{
			if (msg.getHeader().getString(MsgType.FIELD).equals(MsgType.LOGON))
			{
				if (s.getSenderCompID().equals("demo417_md"))
				{
					// Market Data session
			        System.out.println("Market data session");
			        msg.getHeader().setField(new quickfix.field.Username("demo417_md"));
			        msg.getHeader().setField(new quickfix.field.Password("webster7"));
				}
				if (s.getSenderCompID().equals("demo417_om"))
				{
					// Trading session
			        System.out.println("Trading session");
			        msg.getHeader().setField(new quickfix.field.Username("demo417_om"));
			        msg.getHeader().setField(new quickfix.field.Password("webster7"));
				}        		     		
			}
		} 
       	catch (FieldNotFound e) 
       	{
            System.out.println(e.getMessage());
		}        
    }

    public void onMessage(quickfix.fix44.Logon msg, SessionID s) 
    {
        System.out.println(Utils.now() + "Logon from: " + s.toString());
    }

    public void onMessage(quickfix.fix44.Logout msg, SessionID s) 
    { 
        System.out.println(Utils.now() + "Logout from: " + s.toString());
    }

    public void onMessage(quickfix.fix44.Heartbeat msg, SessionID s)
    {
        System.out.println(Utils.now() + "Heartbeat from: " + s.toString());
    }

    public void onMessage(quickfix.fix44.TestRequest msg, SessionID s) 
    { 
        System.out.println(Utils.now() + "TestRequest from: " + s.toString());
    }

    public void onMessage(quickfix.fix44.Reject msg, SessionID s)
    {
        System.out.println(Utils.ANSI_RED + Utils.now() + "Reject from: " + s.toString() + Utils.ANSI_RESET);
        d.Publish(msg, s);
    }

	public void onMessage(quickfix.fix44.QuoteRequestReject msg, SessionID s)
	{
        System.out.println(Utils.ANSI_RED + Utils.now() + "QuoteRequestReject from: " + s.toString() + Utils.ANSI_RESET);
        d.Publish(msg, s);
	}
	
	public void onMessage(quickfix.fix43.Quote msg, SessionID s)
	{
        System.out.println(Utils.now() + "Quote from: " + s.toString());
        d.Publish(msg, s);
	}	
	
	public void onMessage(quickfix.fix44.Quote msg, SessionID s)
	{
        System.out.println(Utils.now() + "Quote from: " + s.toString());
        d.Publish(msg, s);
	}

    public void onMessage(quickfix.fix43.QuoteRequest msg, SessionID s)
    {
        System.out.println(Utils.now() + "QuoteRequest from: " + s.toString());
        d.Publish(msg, s);
    }
    
    public void onMessage(quickfix.fix44.QuoteRequest msg, SessionID s)
    {
        System.out.println(Utils.now() + "QuoteRequest from: " + s.toString());
        d.Publish(msg, s);
    }
    
    public void onMessage(quickfix.fix44.QuoteResponse msg, SessionID s)
    {
        System.out.println(Utils.now() + "QuoteResponse from: " + s.toString());
        d.Publish(msg, s);
    }
	
	public void onMessage(quickfix.fix44.QuoteCancel msg, SessionID s)
	{
        System.out.println(Utils.now() + "QuoteCancel from: " + s.toString());
        d.Publish(msg, s);
	}	
	
	public void onMessage(quickfix.fix44.MarketDataSnapshotFullRefresh msg, SessionID s) 
	{
        System.out.println(Utils.now() + "MarketDataSnapshot from: " + s.toString());
        d.Publish(msg, s);
	}
	
	public void onMessage(quickfix.fix44.MarketDataIncrementalRefresh msg, SessionID s) 
	{
        System.out.println(Utils.now() + "MarketDataRefresh from: " + s.toString());
        d.Publish(msg, s);
	}
	
	public void onMessage(quickfix.fix44.MarketDataRequestReject msg, SessionID s) 
	{
        System.out.println(Utils.now() + "MarketDataReject from: " + s.toString());
        d.Publish(msg, s);
	}
		
	public void onMessage(quickfix.fix43.BusinessMessageReject msg, SessionID s) 
	{
        System.out.println(Utils.ANSI_RED + Utils.now() + "BusinessMessageReject from: " + s.toString() + Utils.ANSI_RESET);
        d.Publish(msg, s);
	}
	
	public void onMessage(quickfix.fix44.BusinessMessageReject msg, SessionID s) 
	{
        System.out.println(Utils.ANSI_RED + Utils.now() + "BusinessMessageReject from: " + s.toString() + Utils.ANSI_RESET);
        d.Publish(msg, s);
	}
	
	public void onMessage(quickfix.fix44.TradingSessionStatus msg, SessionID s) 
	{
        System.out.println(Utils.now() + "TradingSessionStatus from: " + s.toString());
	}		
	
    public void onMessage(quickfix.fix44.OrderCancelReject msg, SessionID s) 
    {
        System.out.println(Utils.now() + "OrderCancelReject from: " + s.toString());
        d.Publish(msg, s);
    }
    
    public void onMessage(quickfix.fix44.NewOrderSingle msg, SessionID s)
    {
        System.out.println(Utils.now() + "NewOrderSingle from: " + s.toString());
        d.Publish(msg, s);
    }        
    
    public void onMessage(quickfix.fix44.ExecutionReport msg, SessionID s) 
    {
        System.out.println(Utils.now() + "ExecutionReport from: " + s.toString());
        d.Publish(msg, s);
    }

    public void onMessage(quickfix.fix44.News msg, SessionID s)
    {
        System.out.println(Utils.now() + "News from: " + s.toString());
        d.Publish(msg, s);
    }

    public void onMessage(quickfix.fix44.SequenceReset msg, SessionID s)
    {
        System.out.println(Utils.now() + "SequenceReset from: " + s.toString());
        // Don't publish to the acceptorQ because this will get local engines confused
    }
}

package eigerfix;

import quickfix.*;

public class EigerFIX 
{
    // static Thread t;

	public static void main(String[] args) throws Exception 
    {
    	System.out.println("In main()");

    	// The freaking windows newline in logs requires correct separator
    	System.setProperty("line.separator", "\r\n");
    	
    	// Set up client side and provider side Disruptors
    	DisruptorClientSide clientSideDisruptor = new DisruptorClientSide(); 
    	DisruptorProviderSide providersideDisruptor = new DisruptorProviderSide(); 
    	
		// Start FIX (client side) Acceptor
    	// It cannot publish to the client side disruptor until the initiator is able to be invoked
		Acceptor a = startAcceptor(args[0], clientSideDisruptor);
		
		// Start FIX (provider side) Initiator
		// It cannot publish to the provider side disruptor until the acceptor is able to be invoked
		Initiator i = startInitiator(args[1], providersideDisruptor);
    	
		// Start the disruptors and pass in the acceptor and initiator
		// Publishing to the disruptors can then begin
		try
	    {        
	    	clientSideDisruptor.start(i);
	    	providersideDisruptor.start(a);
	    	System.out.println("Eiger Disruptors started");
	    }
	    catch (Exception e)
	    {
	    	clientSideDisruptor.stop();
	    	providersideDisruptor.stop();
	    	System.out.println("==DISRUPTORS ERROR==");                
            System.out.println(e.toString());
	    }
  
        // Logging.LogExit();
        System.out.println("Out Main()");    	
    }

	static Acceptor startAcceptor(String arg, DisruptorClientSide clientSideDisruptor)
    {
        Acceptor a = null;
        
        System.out.println("Starting acceptor..."); 
		
		try
        {			          	            	
        	SessionSettings settings = new SessionSettings(arg);
        	System.out.println("settings: " + settings.toString());
            // Logging.LogInfo("settings: " + settings.toString());

			Application app = new FixAcceptorEngine(clientSideDisruptor);
			MessageStoreFactory storeFactory = new FileStoreFactory(settings); 
			LogFactory logFactory = new FileLogFactory(settings); 
			MessageFactory messageFactory = new DefaultMessageFactory(); 
            a = new ThreadedSocketAcceptor(app, storeFactory, settings, logFactory, messageFactory);
						
			a.start();
		}
        catch (Exception e)
        {
            // Logging.LogError("==FATAL ERROR==: {0}", e);
            System.out.println("==FATAL ERROR==");                
            System.out.println(e.toString());
        }
		
		return a;
    }
        
	static Initiator startInitiator(String arg, DisruptorProviderSide providerSideDisruptor)
    {
        Initiator i = null;
        
        System.out.println("Starting initiator..."); 
		
		try
        {	  	
	    	SessionSettings settings = new SessionSettings(arg);
	    	System.out.println("settings: " + settings.toString());
	        // Logging.LogInfo("settings: " + settings.toString());
	
			Application app = new FixInitiatorEngine(providerSideDisruptor);
			MessageStoreFactory storeFactory = new FileStoreFactory(settings); 
			LogFactory logFactory = new FileLogFactory(settings); 
			MessageFactory messageFactory = new DefaultMessageFactory(); 
	        i = new ThreadedSocketInitiator(app, storeFactory, settings, logFactory, messageFactory);
						
			i.start();
		
			SessionID sessionId = i.getSessions().get(0);
			Session.lookupSession(sessionId).logon();
		}
	    catch (Exception e)
	    {
	        // Logging.LogError("==FATAL ERROR==: {0}", e);
	        System.out.println("==FATAL ERROR==");                
	        System.out.println(e.toString());
	    }
		
		return i;
    }	
}



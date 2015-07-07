package eigerfix;

import quickfix.*;

public class EigerFIX 
{
    // static Thread t;
	
	// Setup logging
	static Log l = new Log();

	public static void main(String[] args) throws Exception 
    {
    	System.out.println("In main()");
    	
    	// Setup system out and error
    	// System.setOut(new PrintStream(new File("System.Out")));
    	// System.setOut(new PrintStream(new File("System.Err")));
    	
    	// Set up client side and provider side Disruptors
    	DisruptorFromClients disruptorFromClients = new DisruptorFromClients();
    	DisruptorToProviders disruptorToProviders = new DisruptorToProviders();
    	DisruptorFromProviders disruptorFromProviders = new DisruptorFromProviders();
    	DisruptorToClients disruptorToClients = new DisruptorToClients();
    	
		// Start FIX (client side) Acceptor
    	// It cannot publish to the client side disruptor until the initiator is able to be invoked
		Acceptor a = startAcceptor(args[0], disruptorFromClients);
		
		// Start FIX (provider side) Initiator
		// It cannot publish to the provider side disruptor until the acceptor is able to be invoked
		Initiator i = startInitiator(args[1], disruptorFromProviders);
    	
		// Start the disruptors and pass in the acceptor and initiator
		// Publishing to the disruptors can then begin
		try
	    {        
			// Watch order of starts
			disruptorToProviders.start(i);
			disruptorFromClients.start(a, disruptorToProviders);
	    	disruptorToClients.start(a);
	    	disruptorFromProviders.start(i, disruptorToClients);
	    	System.out.println("Eiger Disruptors started");
	    }
	    catch (Exception e)
	    {
	    	// Doesn't matter order of stops?
	    	disruptorFromClients.stop();
			disruptorToProviders.stop();
	    	disruptorFromProviders.stop();
	    	disruptorToClients.stop();
	    	System.out.println("==DISRUPTORS ERROR==");                
            System.out.println(e.toString());
	    }
  
        // Logging.LogExit();
        System.out.println("Out Main()");    	
    }

	static Acceptor startAcceptor(String arg, DisruptorFromClients disruptorFromClients)
    {
        Acceptor a = null;
        
        System.out.println("Starting acceptor..."); 
		
		try
        {			          	            	
        	SessionSettings settings = new SessionSettings(arg);
        	System.out.println("settings: " + settings.toString());
            // Logging.LogInfo("settings: " + settings.toString());

			Application app = new FixAcceptorEngine(disruptorFromClients);
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
        
	static Initiator startInitiator(String arg, DisruptorFromProviders disruptorFromProviders)
    {
        Initiator i = null;
        
        System.out.println("Starting initiator..."); 
		
		try
        {	  	
	    	SessionSettings settings = new SessionSettings(arg);
	    	System.out.println("settings: " + settings.toString());
	        // Logging.LogInfo("settings: " + settings.toString());
	
			Application app = new FixInitiatorEngine(disruptorFromProviders);
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



package eigerfix;

import java.util.ArrayList;

public class RfqCache
{
	// Init a list of rfq
	static ArrayList<RfqClient> rc = new ArrayList<RfqClient>();
	static ArrayList<RfqQuote> rq = new ArrayList<RfqQuote>();
	static ArrayList<RfqOrder> oc = new ArrayList<RfqOrder>();
	static ArrayList<FixClient> fc = new ArrayList<FixClient>();	
		
	// Set the client for an rfq
	public static void set_rfq(String request, String client, String base_currency, 
			String terms_currency, Double size, String value_date, String account)	
	{
		RfqClient new_rfq = new RfqClient();
		new_rfq.rfq = request;
		new_rfq.client = client;
		new_rfq.base_currency = base_currency;
		new_rfq.terms_currency = terms_currency;
		new_rfq.size = size;
		new_rfq.value_date = value_date;
		new_rfq.account = account;
			
		rc.add(new_rfq);
		
    	System.out.println("set_rfq_client add: " + 
		"rfq: " + request + ", " +
    	"client: "	+ client + ", " +
		"base_currency: " + base_currency + ", " + 	
		"terms_currency: " + terms_currency + ", " + 			
		"size: " + size + ", " + 			
		"value_date: " + value_date + ", " + 			
		"account: " + account);
	}
	
	// Update the list for an rfq or set a new one
	public static void set_rfq_quote(String request, String quote)
	{
		RfqQuote new_rfq = new RfqQuote();
		new_rfq.rfq = request;
		new_rfq.quote = quote;
			
		boolean found_rfq = false;
			
		for (int i=0; i < rq.size(); i++)
		{
			if (rq.get(i).rfq.equals(request))
			{
				// replace the latest quote id
				rq.remove(i);
				rq.add(new_rfq);
				found_rfq = true;
			}
		};
			
		if (found_rfq == false)
		{
			// it's a new rfq
				rq.add(new_rfq);
		}
		
    	System.out.println("set_rfq_quote added: " + request + ", " + quote);
	}
			
	// Get the latest quote id for an rfq
	public static String get_rfq_quote(String request)
	{
		// search the list for the rfq_id
		try
		{
			for (int i=0; i < rq.size(); i++)
			{
				if (rq.get(i).rfq.equals(request))
				{
			    	System.out.println("get_rfq_quote returns: " + request + ", " + rq.get(i).quote);
					return rq.get(i).quote;
				}
			};
		}
		catch (Exception e)
		{
			System.out.println(Utils.now() + "ERROR: aaargh" );
	    	e.printStackTrace();
		}
		
		return "UNKNOWN";
	}
	
	// Get the client for an rfq
	public static String get_rfq_client(String request)
	{
		// search the list for the rfq_id
		try
		{
			for (int i=0; i < rc.size(); i++)
			{
				if (rc.get(i).rfq.equals(request))
				{
			    	System.out.println("get_rfq_client record " + 
			    			"rfq: " + rc.get(i).rfq + ", " +  
			    	    	"client: "	+ rc.get(i).client + ", " +
			    			"base_currency: " + rc.get(i).base_currency + ", " + 	
			    			"terms_currency: " + rc.get(i).terms_currency + ", " + 			
			    			"size: " + rc.get(i).size + ", " + 			
			    			"value_date: " + rc.get(i).value_date + ", " + 			
			    			"account: " + rc.get(i).account);				    			
	    	
					return rc.get(i).client;
				}
			};
		}
		catch (Exception e)
		{
			System.out.println(Utils.now() + "ERROR: aaargh" );
	    	e.printStackTrace();
		}
			
		return "UNKNOWN";
	}
	
	// Get the client for an rfq
	public static String get_rfq_value_date(String request)
	{
		// search the list for the rfq_id
		try
		{
			for (int i=0; i < rc.size(); i++)
			{
				if (rc.get(i).rfq.equals(request))
				{
			    	System.out.println("get_rfq_value_date record " + 
			    			"rfq: " + rc.get(i).rfq + ", " +  
			    	    	"client: "	+ rc.get(i).client + ", " +
			    			"base_currency: " + rc.get(i).base_currency + ", " + 	
			    			"terms_currency: " + rc.get(i).terms_currency + ", " + 			
			    			"size: " + rc.get(i).size + ", " + 			
			    			"value_date: " + rc.get(i).value_date + ", " + 			
			    			"account: " + rc.get(i).account);				    			
	    	
					return rc.get(i).value_date;
				}
			};
		}
		catch (Exception e)
		{
			System.out.println(Utils.now() + "ERROR: aaargh" );
	    	e.printStackTrace();
		}
			
		return "UNKNOWN";
	}	

	// Set the client for an order
	public static void set_order_client(String order, String client) 
	{
		// search the list for the rfq_id
		RfqOrder new_order = new RfqOrder();
		new_order.order = order;
		new_order.client = client;
			
		oc.add(new_order);
    	System.out.println("set_order_client added: " + order + ", " + client);
	}
	
	public static String get_order_client(String order)
	{
		// search the list for the rfq_id
		try
		{
			for (int i=0; i < oc.size(); i++)
			{
				if (oc.get(i).order.equals(order))
				{
					System.out.println("get_order_client returns: " + order + ", " + oc.get(i).client);
					return oc.get(i).client;
				}
			};
		}
		catch (Exception e)
		{
			System.out.println(Utils.now() + "ERROR: aaargh" );
	    	e.printStackTrace();
		}
		
		return "UNKNOWN";
	}

	public static void set_fix_client(int seq, String client)
	{
		FixClient new_fix = new FixClient();
		new_fix.sequence = seq;
		new_fix.client = client;
			
		fc.add(new_fix);
    	System.out.println("set_fix_client added: " + seq + ", " + client);
	}
	
	// Get the latest quote id for an rfq
	public static String get_fix_client(int seq)
	{
		// search the list for the rfq_id
		try
		{
			for (int i=0; i < fc.size(); i++)
			{
				if (fc.get(i).sequence == seq)
				{
					System.out.println("get_fix_client returns: " + seq + ", " + fc.get(i).client);
					return fc.get(i).client;
				}
			};
		}
		catch (Exception e)
		{
			System.out.println(Utils.now() + "ERROR: aaargh" );
	    	e.printStackTrace();
		}
		
		return "UNKNOWN";
	}
}

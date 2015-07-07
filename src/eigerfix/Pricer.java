package eigerfix;

import java.util.ArrayList;

public class Pricer 
{
	// Maintain arrays of prices
	static ArrayList<Price> EURUSD = new ArrayList<Price>();
	static ArrayList<Price> GBPUSD = new ArrayList<Price>();
	static ArrayList<Price> EURGBP = new ArrayList<Price>();
	static ArrayList<Price> USDJPY = new ArrayList<Price>();
	static ArrayList<Price> EURJPY = new ArrayList<Price>();
	static ArrayList<Price> AUDUSD = new ArrayList<Price>();
		
	static void setPrice(Price providerQuote)
	{
		String pair = providerQuote.base_currency + providerQuote.terms_currency;
		
		switch (pair)
		{
			case "EURUSD":
				EURUSD.add(providerQuote);
			case "GBPUSD":
				GBPUSD.add(providerQuote);	
			case "EURGBP":
				EURGBP.add(providerQuote);
			case "USDJPY":
				USDJPY.add(providerQuote);	
			case "EURJPY":
				EURJPY.add(providerQuote);
			case "AUDUSD":
				AUDUSD.add(providerQuote);
		}
		
    	System.out.println("setPrice add: " + 
		providerQuote.base_currency + ", " + 
		providerQuote.terms_currency + ", " +
		providerQuote.latest_quote_id + ", " +
		providerQuote.max_size + ", " +
		providerQuote.quote);
	}
	
	static Price getPrice(String base_currency, String terms_currency, double size)
	{
		String pair = base_currency + terms_currency;
		Price p = new Price();		
		
		try
		{
			switch (pair)
			{
				case "EURUSD":
					p = EURUSD.get(EURUSD.size() - 1);
				case "GBPUSD":
					p = GBPUSD.get(GBPUSD.size() - 1);
				case "EURGBP":
					p = EURGBP.get(EURGBP.size() - 1);
				case "USDJPY":
					p = USDJPY.get(USDJPY.size() - 1);
				case "EURJPY":
					p = EURJPY.get(EURJPY.size() - 1);
				case "AUDUSD":
					p = AUDUSD.get(GBPUSD.size() - 1);
			}	
		}
		catch (Exception e)
		{
			System.out.println(Utils.now() + "ERROR: aaargh" );
	    	e.printStackTrace();
		}
		
    	System.out.println("getPrice get: " + 
		p.base_currency + ", " + 
		p.terms_currency + ", " +
		p.latest_quote_id + ", " +
		p.max_size + ", " +
		p.quote);		
		
		return p;
	}	
}

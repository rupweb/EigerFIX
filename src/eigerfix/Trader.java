package eigerfix;

public class Trader
{
	// This class handles the pricing skews and spreads
	// We add them into a provider price (call it deal)
	// We subtract them from a client order (call it cover)
	
	public static Price deal(Price p)
	{
		// Add a spread
		p.bid = p.bid - 0.0001;
		p.offer = p.offer + 0.0001;	
		
		return p;
	}

	public static Order cover(Order o)
	{
		// Subtract a spread
		o.bid = o.bid + 0.0001;
		o.offer = o.offer - 0.0001;
		
		return o;
	}	
	
}

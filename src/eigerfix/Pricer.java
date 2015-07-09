package eigerfix;

import java.util.ArrayList;

import quickfix.FieldNotFound;
import quickfix.SessionID;
import quickfix.field.BidPx;
import quickfix.field.BidSpotRate;
import quickfix.field.MidPx;
import quickfix.field.OfferPx;
import quickfix.field.OfferSpotRate;
import quickfix.field.QuoteID;
import quickfix.field.QuoteReqID;
import quickfix.field.SettlDate;
import quickfix.field.SettlType;
import quickfix.field.Symbol;
import quickfix.field.ValidUntilTime;
import quickfix.fix44.Quote;

public class Pricer
{
	// Maintain arrays of prices
	static ArrayList<Price> EURUSD = new ArrayList<Price>();
	static ArrayList<Price> GBPUSD = new ArrayList<Price>();
	static ArrayList<Price> EURGBP = new ArrayList<Price>();
	static ArrayList<Price> USDJPY = new ArrayList<Price>();
	static ArrayList<Price> EURJPY = new ArrayList<Price>();
	static ArrayList<Price> AUDUSD = new ArrayList<Price>();
	
	static void setPrice(Price p, DisruptorToClients d)
	{
		String pair = p.base_currency + p.terms_currency;
		
		switch (pair)
		{
			case "EURUSD":
				EURUSD.add(p);
				break;
			case "GBPUSD":
				GBPUSD.add(p);
				break;
			case "EURGBP":
				EURGBP.add(p);
				break;
			case "USDJPY":
				USDJPY.add(p);
				break;
			case "EURJPY":
				EURJPY.add(p);
				break;
			case "AUDUSD":
				AUDUSD.add(p);
				break;
		}
		
    	System.out.println("setPrice add: " + 
		"base: " + p.base_currency + ", " + 
		"terms: " + p.terms_currency + ", " +
		"rfq id: " + p.rfq_id + ", " +
		"quote id: " + p.quote_id + ", " +
		"max size: " + p.max_size + ", " +
		"bid: " + p.bid + ", " +
		"offer: " + p.offer + ", " +
		"bid spot: " + p.bid_spot_rate + ", " +
		"offer spot: " + p.offer_spot_rate + ", " +
		"value: " + p.value_date);
    	
    	// Skew or spread the price depending on Eiger configurator
    	p = Trader.deal(p);
    	
    	// Now that a price is set, then publish a FIX quote to the ToClients disruptor
    	publishQuote(p, d);
	}
	
	private static void publishQuote(Price p, DisruptorToClients d)
	{
		// Make a Quote message for this pair
		QuoteReqID qrID = new QuoteReqID(p.rfq_id);
		QuoteID qID = new QuoteID(p.quote_id);
		Symbol s = new Symbol(p.base_currency + "/" + p.terms_currency);
		ValidUntilTime vut = new ValidUntilTime();
		SettlType st = new SettlType("B");
		SettlDate sd = new SettlDate(p.value_date);
		BidPx bp = new BidPx(p.bid);
		OfferPx op = new OfferPx(p.offer);
		BidSpotRate bsr = new BidSpotRate(p.bid_spot_rate);
		OfferSpotRate osr = new OfferSpotRate(p.offer_spot_rate);
		MidPx mp = new MidPx();
		
		Quote m = new Quote();
		
		m.setField(qrID);
		m.setField(qID);
		m.setField(s);
		m.setField(vut);
		m.setField(st);
		m.setField(sd);
		m.setField(bp);
		m.setField(op);
		m.setField(bsr);
		m.setField(osr);
		m.setField(mp);
		
		// Figure out all the client sessions who want to hear about this quote
		try 
		{
			getSessions(m, d);
		} 
		catch (FieldNotFound e) 
		{
			System.out.println(Utils.now() + "ERROR: aaargh" );
			e.printStackTrace();
		}
	}

	private static void getSessions(Quote m, DisruptorToClients d) throws FieldNotFound 
	{
        // Get the client to send this quote back to   	
    	String request = m.getField(new QuoteReqID()).getValue();
    	String client = RfqCache.get_rfq_client(request);
    	 	
        // Replace the session id with the new client
    	SessionID session = new SessionID("FIX.4.4", "EIGER_FX", client);
    	
    	d.Publish(m, session);
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
		"base: " + p.base_currency + ", " + 
		"terms: " + p.terms_currency + ", " +
		"rfq id: " + p.rfq_id + ", " +
		"quote id: " + p.quote_id + ", " +
		"max size: " + p.max_size + ", " +
		"bid: " + p.bid + ", " +
		"offer: " + p.offer + ", " +
		"bid spot: " + p.bid_spot_rate + ", " +
		"offer spot: " + p.offer_spot_rate + ", " +
		"value: " + p.value_date);
		
		return p;
	}	
}

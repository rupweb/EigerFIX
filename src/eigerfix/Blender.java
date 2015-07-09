package eigerfix;

import quickfix.FieldNotFound;
import quickfix.Group;
import quickfix.IntField;
import quickfix.StringField;
import quickfix.field.Account;
import quickfix.field.Currency;
import quickfix.field.MDEntryType;
import quickfix.field.MDReqID;
import quickfix.field.MarketDepth;
import quickfix.field.OrderQty;
import quickfix.field.QuoteReqID;
import quickfix.field.QuoteRequestType;
import quickfix.field.SettlDate;
import quickfix.field.SettlType;
import quickfix.field.SubscriptionRequestType;
import quickfix.field.Symbol;
import quickfix.fix44.MarketDataRequest;
import quickfix.fix44.MarketDataRequest.NoMDEntryTypes;
import quickfix.fix44.MarketDataRequest.NoRelatedSym;

public class Blender 
{
	public static quickfix.fix43.QuoteRequest RFQ43(quickfix.fix43.QuoteRequest m) throws FieldNotFound 
	{
    	// Now rebuild the Quote Request in a different tag order and grouping    	
 			
		// Take incoming group without using data dictionary
		Group g = m.getGroup(1, 146);
		
		// Setup outgoing group and specify an index for each group tag
		Group g2 = new Group(146, 1);
		
		// There's a casting problem and you have to go through the hoops
		try 
		{ 
			Symbol symbol = new Symbol();
			StringField symbol1 = g.getField(new StringField(55));
			symbol = new Symbol(symbol1.getValue());		
			m.setField(symbol);
		}
		catch (quickfix.FieldNotFound e) {}

		// deal with voluntary tags
		try 
		{ 
		  	QuoteRequestType qt = new QuoteRequestType();
			IntField qt1 = g.getField(new IntField(303));
		  	qt = new QuoteRequestType(qt1.getValue());
			g2.setField(1, qt);
		}
		catch (quickfix.FieldNotFound e) {}
		
		try 
		{ 
		  	OrderQty amount = new OrderQty();
			amount = (OrderQty) g.getField(amount);
			g2.setField(2, amount);
		}
		catch (quickfix.FieldNotFound e) {}	
		
		try 
		{ 
		  	SettlType sType = new SettlType();
			StringField sType1 = g.getField(new StringField(63));
			sType = new SettlType(sType1.getValue());
			g2.setField(3, sType);
		}
		catch (quickfix.FieldNotFound e) {}
		
		try 
		{ 
		  	SettlDate sDate = new SettlDate();
			StringField sDate1 = g.getField(new StringField(64));
			sDate = new SettlDate(sDate1.getValue());
			g2.setField(4, sDate);
		}
		catch (quickfix.FieldNotFound e) {}

		try 
		{ 
			Currency ccy = new Currency();
			StringField ccy1 = g.getField(new StringField(15));
			ccy = new Currency(ccy1.getValue());
			g2.setField(5, ccy);
		}
		catch (quickfix.FieldNotFound e) {}
		
		try 
		{ 
		  	Account account = new Account();
		  	StringField account1 = g.getField(new StringField(1));
		  	account = new Account(account1.getValue());
		  	g2.setField(6, account);
		}
		catch (quickfix.FieldNotFound e) {}  	

		// replace old group
		m.replaceGroup(1, g2);
		return null;
	}
	
	public static quickfix.fix44.QuoteRequest RFQ44(quickfix.fix44.QuoteRequest m) throws FieldNotFound
	{	
		// Rebuild the Quote Request in a different tag order and grouping  	
		// Setup tag types
		Symbol symbol = new Symbol();
		OrderQty amount = new OrderQty();
		Account account = new Account();
		Currency ccy = new Currency();
		SettlType sType = new SettlType();
		SettlDate sDate = new SettlDate();
		QuoteRequestType qt = new QuoteRequestType();
				
		// Take group without using data dictionary
		Group g = m.getGroup(1, 146); 
		
		// There's a casting problem and you have to go through the hoops
		StringField symbol1 = g.getField(symbol);
		IntField qt1 = g.getField(qt);
		StringField sType1 = g.getField(sType);		
		StringField sDate1 = g.getField(sDate);
		StringField ccy1 = g.getField(ccy);
		StringField account1 = g.getField(account);		
		
		// refresh data		
		symbol = new Symbol(symbol1.getValue());
		qt = new QuoteRequestType(qt1.getValue());
		amount = (OrderQty) g.getField(amount);
		sType = new SettlType(sType1.getValue());		
		sDate = new SettlDate(sDate1.getValue());
		ccy = new Currency(ccy1.getValue());
		account = new Account(account1.getValue());
	
		// Populate tags
		m.setField(symbol);
		
		// Setup a group and specify an index for each group tag
		Group g2 = new Group(146, 1);
		g2.setField(1, qt);	
		g2.setField(2, amount);
		g2.setField(3, sType);
		g2.setField(4, sDate);
		g2.setField(5, ccy);
		g2.setField(6, account);
		
		// replace old group
		m.replaceGroup(1, g2);
		
		return m;
	}

	public static MarketDataRequest RFQtoMD(quickfix.fix44.QuoteRequest m) throws FieldNotFound 
	{
		// You can reuse the same market data request id reference
		// So it's recommended to use the symbol, i.e. EURUSD to avoid duplicate streams
		String id = m.get(new QuoteReqID()).getValue();
		
		MDReqID mdReqID = new MDReqID(id);
		SubscriptionRequestType subType = new SubscriptionRequestType(SubscriptionRequestType.SNAPSHOT);
		MarketDepth marketDepth = new MarketDepth(1);
			
		MarketDataRequest message = new MarketDataRequest(mdReqID, subType, marketDepth);
            
		// regardless of bid or offer GAIN will send both sides back
		NoMDEntryTypes marketDataEntryGroup = new NoMDEntryTypes();
		marketDataEntryGroup.set(new MDEntryType(MDEntryType.BID));

		// GainGTX requires the Symbol to be in format like: EUR/USD
		NoRelatedSym symbolGroup = new NoRelatedSym();
		symbolGroup.set((Symbol) m.getField(new Symbol()));
			
		message.addGroup(marketDataEntryGroup);
		message.addGroup(symbolGroup);
	            
		// Give security type for foreign exchange contract
		// SecurityType securityType = new SecurityType("FOR");            
		// message.SetField(securityType);
	                       		
		// SettlDate tag 64 set to blank so it is returned with correct spot date
		// Group g = m.getGroup(1, 146);	
		// message.setField(new StringField(64, g.getString(4)));
		
		// GainGTX custom tag 7820 set to value date in DDMMYYYY form or blank for SPOT
		message.setField(new StringField(7820, ""));
	
		return message;
	}
}

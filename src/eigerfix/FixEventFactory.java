package eigerfix;

import quickfix.Message;

import com.lmax.disruptor.EventFactory;

public class FixEventFactory implements EventFactory<FixEvent>
{
	@Override
	public FixEvent newInstance() 
	{
        FixEvent e = new FixEvent();
        e.message = new Message();
        
    	// System.out.println("Created FixEvent.newInstance " +  e.message.toString());       	
    	
        return e;
	}
}

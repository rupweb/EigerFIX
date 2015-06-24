package eigerfix;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.dsl.Disruptor;

/**
 * Implements the receive loop; this thread will never end until interrupted
 */

public class ReceiveThread implements Runnable 
{
    @SuppressWarnings("rawtypes")
	private final Disruptor disruptor;
    
    @SuppressWarnings("rawtypes")
	public ReceiveThread(Disruptor disruptor)
    {
    	System.out.println("In ReceiveThread.run() with disruptor: " + disruptor.toString());
        this.disruptor = disruptor;
    }
    
    @SuppressWarnings("unchecked")
	public void run() 
    {
    	System.out.println("In ReceiveThread.run()");

        while(true) 
		{
			System.out.println("ReceiveThread block...");     		
			// block to receive and wait for next round

			disruptor.publishEvent(new EventTranslator<FixEvent>() 
			{
				public void translateTo(FixEvent event, long sequence) 
				{
					System.out.println("In ReceiveThread.run.publishEvent.translateTo()");
				}
			});
		}
    }
}


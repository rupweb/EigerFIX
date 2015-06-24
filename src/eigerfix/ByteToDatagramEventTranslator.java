package eigerfix;

import quickfix.Message;

import com.lmax.disruptor.EventTranslatorOneArg;

/**
 * Pushes an output event onto the target disruptor
 * <p>
 * This uses the EventTranslatorTwoArg which has a special publishEvent facility on the ringbuffer
 * to avoid generating any garbage
 */
public class ByteToDatagramEventTranslator implements EventTranslatorOneArg<FixEvent, Message> 
{
    public void translateTo(FixEvent event, long sequence, Message m) 
    {
    	System.out.println("In ByteToDatagramEventTranslator.translateTo()");
        event.message = m;
    	System.out.println("event.message: " + event.message);
    }
}
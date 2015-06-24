package eigerfix;

import com.lmax.disruptor.EventTranslator;

/**
 * Pushes an output event onto the target disruptor
 * <p>
 * This uses the EventTranslatorOneArg which has a special publishEvent facility on the ringbuffer
 * to avoid generating any garbage
 */

public class Translator implements EventTranslator<FixEvent> 
{
	public void translateTo(FixEvent event, long sequence) 
	{
		System.out.println("In Translator.translateTo()");
	}

}

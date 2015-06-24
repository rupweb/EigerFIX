package eigerfix;

import quickfix.Message;
import quickfix.SessionID;

import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;

public class FixEventProducerWithTranslator 
{
	private final RingBuffer<FixEvent> ringBuffer;

	public FixEventProducerWithTranslator(RingBuffer<FixEvent> ringBuffer)
	{
		this.ringBuffer = ringBuffer;
	}

	private static final EventTranslatorTwoArg<FixEvent, Message, SessionID> TRANSLATOR =
			new EventTranslatorTwoArg<FixEvent, Message, SessionID>()
	{
		public void translateTo(FixEvent event, long sequence, Message m, SessionID s)
		{
			event.set(m, s);
		}
	};

	public void onData(Message m, SessionID s)
	{
		ringBuffer.publishEvent(TRANSLATOR, m, s);
	}
}

package eigerfix;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import quickfix.Initiator;
import quickfix.Message;
import quickfix.SessionID;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class DisruptorFromProviders 
{    
    private static final int RING_SIZE = 1*1024;
    static final int BYTE_ARRAY_SIZE = 1*1024;

    Disruptor<FixEvent> disruptor;
    ExecutorService executor;
    Thread t;

	public DisruptorFromProviders() 
    {
    	System.out.println("In DisruptorFromProviders()");
    }

    @SuppressWarnings("unchecked")
	public void start(Initiator i, DisruptorToClients d) throws Exception 
    {	
    	System.out.println("In DisruptorFromProviders.start()");
    	
    	int NUM_EVENT_PROCESSORS = 5;

        executor = Executors.newFixedThreadPool(NUM_EVENT_PROCESSORS);
        
        FixEventFactory factory = new FixEventFactory();

    	System.out.println("Starting Disruptor From Providers");
        disruptor = new Disruptor<>(factory, RING_SIZE, executor, ProducerType.SINGLE, new BlockingWaitStrategy());
        disruptor.handleEventsWith(new Log(), new Replicator(), new DisruptorFromProvidersLogic(disruptor, i, d));
        disruptor.start();

        // System.out.println("Setup Router");

        // Start listening to the disruptor
        // t = new Thread(new Router(disruptor));
        // t.start();

        System.out.println("DisruptorFromProviders listening...");
    }

    public void stop() throws Exception 
    {
    	System.out.println("In DisruptorFromProviders.stop()");

        // early exit
        if (t == null) return;
              
    	System.out.println("Interrupting ReceiveThread: " + t.toString());
        t.interrupt();
        
    	System.out.println("Joining ReceiveThread: " + t.toString());
        t.join();

    	System.out.println("Shutting down executor: " + executor.toString());
        executor.shutdown();
        t = null;
        
    	System.out.println("Out DisruptorFromProviders.stop()");
    }
    
	void Publish(Message m, SessionID s)
    {		
    	System.out.println(Utils.now() + Utils.ANSI_CYAN + "PUBLISH: DisruptorFromProviders " + m.toString() + Utils.ANSI_RESET);
		
		// Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<FixEvent> ringBuffer = disruptor.getRingBuffer();

        FixEventProducer producer = new FixEventProducer(ringBuffer);

        producer.onData(m, s);
        
    	System.out.println(Utils.now() + "Out DisruptorFromProviders.Publish()");
    }
	
}

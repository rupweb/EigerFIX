package eigerfix;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import quickfix.Acceptor;
import quickfix.Message;
import quickfix.SessionID;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class DisruptorFromClients 
{    
    private static final int RING_SIZE = 1*1024;
    static final int BYTE_ARRAY_SIZE = 1*1024;

    Disruptor<FixEvent> disruptor;
    ExecutorService executor;
    Thread t;

	public DisruptorFromClients() 
    {
    	System.out.println("In DisruptorFromClients()");
    }

    @SuppressWarnings("unchecked")
	public void start(Acceptor a, DisruptorToProviders d) throws Exception 
    {	
    	System.out.println("In DisruptorFromClients.start()");
    	
    	int NUM_EVENT_PROCESSORS = 5;

        executor = Executors.newFixedThreadPool(NUM_EVENT_PROCESSORS);
        
        FixEventFactory factory = new FixEventFactory();

    	System.out.println("Starting Disruptor From Clients");
        disruptor = new Disruptor<>(factory, RING_SIZE, executor, ProducerType.SINGLE, new BlockingWaitStrategy());
        disruptor.handleEventsWith(new Log(), new Replicator(), new DisruptorFromClientsLogic(disruptor, a, d));
        disruptor.start();

        // System.out.println("Setup Router");

        // Start listening to the disruptor
        // t = new Thread(new Router(disruptor));
        // t.start();

        System.out.println("DisruptorFromClients listening...");
    }

    public void stop() throws Exception 
    {
    	System.out.println("In DisruptorFromClients.stop()");

        // early exit
        if (t == null) return;
              
    	System.out.println("Interrupting ReceiveThread: " + t.toString());
        t.interrupt();
        
    	System.out.println("Joining ReceiveThread: " + t.toString());
        t.join();

    	System.out.println("Shutting down executor: " + executor.toString());
        executor.shutdown();
        t = null;
        
    	System.out.println("Out DisruptorFromClients.stop()");
    }
    
	void Publish(Message m, SessionID s)
    {		
    	System.out.println(Utils.now() + Utils.ANSI_GREEN + "PUBLISH: DisruptorFromClients " + m.toString() + Utils.ANSI_RESET);
		
		// Get the ring buffer from the Disruptor to be used for publishing.
        RingBuffer<FixEvent> ringBuffer = disruptor.getRingBuffer();

        FixEventProducer producer = new FixEventProducer(ringBuffer);

        producer.onData(m, s);
        
        System.out.println(Utils.now() + "Out DisruptorFromClients.Publish()");
    }
	
}

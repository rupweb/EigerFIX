package eigerfix;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketException;

import com.lmax.disruptor.EventHandler;

import com.hazelcast.core.Hazelcast; 
import com.hazelcast.core.HazelcastInstance; 
import com.hazelcast.core.ITopic; 


// The replicator forwards JSONified events to hazelcast. 
// Used by the monitor server as well.

public class Replicator implements EventHandler<FixEvent> 
{
    public byte[] buffer;
    public SocketAddress address;
    public int length;
    HazelcastInstance hz;
	
    public Replicator() throws IOException, SocketException 
    {
    	System.out.println("In Replicator()");
    	System.out.println("Starting Hazelcast");
		// hz = Hazelcast.newHazelcastInstance();
    	System.out.println("Hazelcast UP");		
    }		

	@Override
	public void onEvent(FixEvent event, long sequence, boolean endOfBatch) throws Exception 
	{
    	System.out.println(Utils.now() + "EVENT: Replication " + event.message.toString());
   	
    	// TODO: for the moment just use text. Maybe JSONify later?
    	// HazelPublisher(event.message.toString());
	}
	
	public void HazelPublisher(String event)
	{ 
		ITopic<String> topic = hz.getTopic("fix"); 
		topic.publish(event); 
	} 

}

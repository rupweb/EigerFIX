package eigerfix;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.SocketException;

import com.lmax.disruptor.EventHandler;

public class _Replicator implements EventHandler<FixEvent> 
{
    public byte[] buffer;
    public SocketAddress address;
    public int length;
	
    public _Replicator() throws IOException, SocketException 
    {
    	System.out.println("In Replicator()");
    }		

	@Override
	public void onEvent(FixEvent event, long sequence, boolean endOfBatch) throws Exception 
	{
    	System.out.println(Utils.now() + "EVENT: Replication " + event.message.toString());
   	
        buffer = new byte[DisruptorProviderSide.BYTE_ARRAY_SIZE];
        length = 0;
    	
    	String endpointName = "127.0.0.1";
    	
    	// Create a non infinite replication loop!
    	int port = 9998;
    	   	
        final DatagramSocket clientSocket = new DatagramSocket();
        clientSocket.setSoTimeout(1000);
  
        final InetAddress endpoint = InetAddress.getByName(endpointName);

        final DatagramPacket sendPacket = new DatagramPacket(buffer, length, endpoint, port);

        // Send to the replicator place
        clientSocket.send(sendPacket);
        
        clientSocket.close();
	}
}

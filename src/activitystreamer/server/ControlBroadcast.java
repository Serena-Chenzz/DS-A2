package activitystreamer.server;

public class ControlBroadcast extends Control{

	 public synchronized static boolean broadcastClients(String msg) {
		for(Connection con : getConnectionClients()) {
			log.debug("con "+con);
			con.writeMsg(msg);
		}
        // need failure model
        return true;

    }

	public static void printConnections() {
		// TODO Auto-generated method stub
		log.debug("TotalConnections: "+connections.size()+" Servers: "+connectionServers.size() +
				" Clients: "+getConnectionClients().size());
	}
	
}

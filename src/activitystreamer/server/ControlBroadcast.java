package activitystreamer.server;

public class ControlBroadcast extends Control{

	 public synchronized static boolean broadcastClients(String msg, long msgTime) {
		for(Connection con : Control.getInstance().getUserConnections().keySet()) {
		    String timeString = Control.getInstance().getUserConnections().get(con).split(" ")[1];
		    long userLoginTime=Long.parseLong(timeString);
		    if (userLoginTime <= msgTime){
    			log.debug("con "+con);
    			con.writeMsg(msg);
    		}
		}
        // need failure model
        return true;

    }

	public static void printConnections() {
		log.debug("TotalConnections: "+connections.size()+" Servers: "+connectionServers.size() +
				" Clients: "+getConnectionClients().size());
	}
	
}

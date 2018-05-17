package activitystreamer.server.commands;

import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import activitystreamer.server.Connection;
import activitystreamer.server.Control;
import activitystreamer.util.Settings;

import activitystreamer.models.*;
import activitystreamer.server.Load;

public class ServerAnnounce extends Thread{
    
    private static boolean closeConnection=false;
    private final static Logger log = LogManager.getLogger();
    
    public ServerAnnounce() {
    	//start();
    }
    
    
    @Override
    public void run() {
    	int load = 0;
    	ArrayList<Connection> connections = Control.getInstance().getConnections();
    	while(!Control.getInstance().getTerm()){
				// do something with 5 second intervals in between
				try {
					load = Load.getOwnLoad();
					JSONObject serverAnnounce = Command.createServerAnnounce(Control.getInstance().getUniqueId(),load,Settings.getLocalHostname(),Settings.getLocalPort()); 
					Control.getInstance().broadcast(serverAnnounce.toJSONString());
					Thread.sleep(Settings.getActivityInterval());
				} catch (InterruptedException e) {
					log.info("received an interrupt, system is shutting down");
					break;
				}
			}
	   	if(!Control.getInstance().getTerm()){
			log.debug("doing activity");
			Control.getInstance().setTerm(doActivity());
		}
		
		log.info("closing "+connections.size()+" connections");
		// clean up
		for(Connection connection : connections){
			connection.closeCon();
		}
		Control.getInstance().listenAgain();;
    }
    
    public static boolean doActivity() {
        return false;
    }
    public static boolean getResponse() {
        return closeConnection;
    }


}

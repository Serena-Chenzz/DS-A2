/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package activitystreamer.server;

import activitystreamer.models.Command;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import org.json.simple.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 *
 * @author wknje
 */
public class Load {

    class ServerInfo {

        private String hostname;
        private String portStr;

        public ServerInfo(String hostname, String portStr) {
            this.hostname = hostname;
            this.portStr = portStr;
        }

    }

    // If the server has more than limited clients, it should consider 
//    private static final int upperLimit = 5;
    private static final Logger log = LogManager.getLogger();
    private Map<String, Integer> loadMap;
    private Map<String,ServerInfo> serverList;

    public Load() {
        this.loadMap = new HashMap<String, Integer>();
        this.serverList = new HashMap<String, ServerInfo>();
    }


    public void updateLoad(JSONObject userInput) {
        String hostname = userInput.get("hostname").toString();
        String portStr = userInput.get("port").toString();
        String id = userInput.get("id").toString();
        // If this is a new server
        if(!serverList.containsKey(id)){
            ServerInfo si = new ServerInfo(hostname, portStr);
            serverList.put(id, si);
        }
        // Update loadmap
        loadMap.put(id, Integer.parseInt(userInput.get("load").toString()));
        
        String output = "";
        for (Map.Entry<String, Integer> entry : loadMap.entrySet()) {
            output = output + entry.getKey() + ':' + entry.getValue() + '\n';
        }
        //log.debug("Current Load Infomation: " + output);
    }

    // Check if 
    public synchronized boolean checkRedirect(Connection clientCon) {
        int ownLoad = getOwnLoad() ;
        // search for the server that is at least 2 clients less than its own
        for (Map.Entry<String, Integer> entry : loadMap.entrySet()) {
            String id = entry.getKey();
            Integer load = entry.getValue();
            if (load  < ownLoad - 2) {
                String hostname = serverList.get(id).hostname;
                String portStr = serverList.get(id).portStr;
                // send redirect infomation to client
//                log.debug("Redirect to " + 
//                        Connection.generateRemoteId(hostname, portStr));
                clientCon.writeMsg(Command.createRedirect(hostname, portStr));
                // Then the connection needs to be terminated
                return true;
            }
        }
        // the connection needn't to be terminated
        return false;
    }
    
    
    

    
    public synchronized static int getOwnLoad(){
        return Control.getInstance().getConnections().size()-Control.getInstance().getConnectionServers().size();
    }

}

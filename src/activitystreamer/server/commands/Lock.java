package activitystreamer.server.commands;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.models.*;
import activitystreamer.server.Connection;
import activitystreamer.server.Control;

public class Lock {

    private static final Logger log = LogManager.getLogger();
    private boolean closeConnection = false;

    public Lock(String msg, Connection con) {

        try {
            //First, I need to check whether this connection is authenticated.
            //If not authenticated, it will return a invalid message
            if (!Control.getInstance().containsServer(con.getRemoteId())) {
                String info = "Lock_Request is not from an authenticated server";
                con.writeMsg(Command.createInvalidMessage(info));
                closeConnection = true;
            } else {
                JSONParser parser = new JSONParser();
                JSONObject message = (JSONObject) parser.parse(msg);
                String username = message.get("username").toString();
                String secret =  message.get("secret").toString();
                
                //First, it will write a "Lock_Request" to connectionServer hashmap
                Control.getInstance().checkAllLocks(con, msg);
                
                //Check if this user exists
                if (Control.getInstance().checkLocalUser(username)) {
                    
                    JSONObject lockDenied = Command.createLockDenied(username, secret);
                    //broadcast this Lock_denied message to all neighbours
                    Control.getInstance().broadcast(lockDenied.toJSONString(), "");

                } else {
                    //If the user is not in the local storage,
                    //Add it to the userlist
                    //Keep sending lock_request to others if it has other neighbour servers
                    //If it is the end node, then it returns a lock_allowed
                    Control.getInstance().addLocalUser(username, secret);
                    
                    if (Control.getInstance().getConnectionServers().size()==1){
                        JSONObject lockAllowed = Command.createLockAllowed(username, secret);
                        con.writeMsg(lockAllowed.toJSONString());
                        closeConnection = false;
                    }
                    else{
                        Control.getInstance().broadcast(msg, con.getRemoteId());
                    }
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    public boolean getCloseCon() {
        return closeConnection;
    }
}

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
                
                //Check if this user exists
                if (Control.getInstance().checkLocalUser(username)) {
                    
                    JSONObject lockDenied = Command.createLockDenied(username, secret);
                    //broadcast this Lock_denied message to all neighbours
                    Control.getInstance().broadcast(lockDenied.toJSONString());

                } else {
                    //If the user is not in the local storage,
                    //Send back lock_allowed
                    JSONObject lockAllowed = Command.createLockAllowed(username, secret);
                    con.writeMsg(lockAllowed.toJSONString());
                    closeConnection = false;
             
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

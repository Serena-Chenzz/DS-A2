package activitystreamer.server.commands;

import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.models.Command;
import activitystreamer.models.User;
import activitystreamer.server.Connection;
import activitystreamer.server.Control;
import activitystreamer.models.*;

public class LockDenied {

	private final Logger log = LogManager.getLogger();
	private boolean closeConnection=false;

	
	public LockDenied(String msg, Connection con) {
		try{
		  //First, I need to check whether this connection is authenticated.
            //If not authenticated, it will return a invalid message
            if (!Control.getInstance().containsServer(con.getRemoteId())) {
                String info = "Lock_Request is not from an authenticated server";
                con.writeMsg(Command.createInvalidMessage(info));
                closeConnection = true;
            }
            else{
                JSONParser parser = new JSONParser();
                JSONObject message = (JSONObject)parser.parse(msg);
                String username= message.get("username").toString();
                String secret= message.get("secret").toString();
                
                //Record the status to this server
                //Delete this user from local userlist
                //Delete this user from the pending list
                //And broadcast the lock_denied
                Control.getInstance().checkAllLocks(con,msg);
                Control.getInstance().deleteLocalUser(username,secret);
                Control.getInstance().deleteFromPendingList(username, secret);
                Control.getInstance().broadcast(msg,con.getRemoteId());
                closeConnection = false;
                }
        }catch(ParseException e){
            e.printStackTrace();
        }
	}
	
	public boolean getCloseCon() {
		return closeConnection;
	}
}


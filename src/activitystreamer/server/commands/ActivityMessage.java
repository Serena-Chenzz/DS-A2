package activitystreamer.server.commands;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.models.Command;
import activitystreamer.server.Connection;
import activitystreamer.server.Control;
import activitystreamer.server.ControlBroadcast;
import activitystreamer.server.Message;

public class ActivityMessage {
	
	private boolean closeConnection=false;
	private final Logger log = LogManager.getLogger();

	@SuppressWarnings("unchecked")
    public ActivityMessage(Connection con, String msg) {
		
		JSONParser parser = new JSONParser();
        JSONObject message;
        try {
            message = (JSONObject) parser.parse(msg);
        	String username = message.get("username").toString();
        	JSONObject activity = (JSONObject) message.get("activity");
        	activity.put("authenticated_user", username);
            //If it is anonymous user, we can ignore the secret field
            String secret = "";
            if (!username.equals("anonymous")){
                secret = message.get("secret").toString();
            }
            log.debug("check :"+username+"/"+secret);
            if(username.equals("anonymous")) {//Anonymous logins
        		//Create a message and store them inside the queues
        		Message newMsg = new Message(con, activity);
        		Control.getInstance().addMessageToBufferQueue(newMsg, con);
        		//Broadcast this new message to servers
        		String actBroad = Command.createActivityServerBroadcast(newMsg);
        		Control.getInstance().broadcast(actBroad);
        		String actBroadClient=Command.createActivityBroadcast(activity);
        		ControlBroadcast.broadcastClients(actBroadClient,newMsg.getTimeStamp());
                closeConnection = false;
                return;
            }
            if(Login.checkUserLoggedIn(username)) {
            	//Start checking users
                if(Control.getInstance().checkLocalUserAndSecret(username,secret)) {  
                	
            		
            		//Create a message and store them inside the queues
                    Message newMsg2 = new Message(con, activity);
                    Control.getInstance().addMessageToBufferQueue(newMsg2, con);
                    
                    //broadCast jsonString
                    String actBroad = Command.createActivityServerBroadcast(newMsg2);
                    Control.getInstance().broadcast(actBroad);
                    String actBroadClient=Command.createActivityBroadcast(activity);
                    ControlBroadcast.broadcastClients(actBroadClient,newMsg2.getTimeStamp());
                    closeConnection = false;
                    return;
                }else { 
                    //If this username and secret are not correct, we send an authentication failed
                    con.writeMsg(Command.createAuthenticateFailed(secret, ""));
                    closeConnection = true;
                    return;
                }
            }else { 
                //If this username and secret are not correct, we send an authentication failed
                con.writeMsg(Command.createAuthFailedUserNotLoggedIn(username));
                closeConnection = true;
                return;
            }
            
            
        } catch (ParseException e) {
        	Command.createInvalidMessage("JSON parse error while parsing message");
        	closeConnection=true;
        	return;
        }
}
	
	public boolean getResponse() {
		return closeConnection;
	}

}
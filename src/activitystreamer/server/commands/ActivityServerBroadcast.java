package activitystreamer.server.commands;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.models.Command;
import activitystreamer.server.Connection;
import activitystreamer.server.Control;
import activitystreamer.server.Message;

public class ActivityServerBroadcast {

	private boolean closeConnection=false;
	private final Logger log = LogManager.getLogger();

	public ActivityServerBroadcast(Connection con, String msg) {
		JSONParser parser = new JSONParser();
        JSONObject message;
        try {
        	//check that JSON format is valid
            message = (JSONObject) parser.parse(msg);
            //Check that message is received from an Authenticated server
            if (!Control.getInstance().containsServer(con.getRemoteId())) {
                String info = "Lock_Request is not from an authenticated server";
                con.writeMsg(Command.createInvalidMessage(info));
                closeConnection = true;
            } else {
                //Send back an acknowledgment to the server which sent the activity message
                long timestamp = (long)message.get("timestamp");
                String senderIp = (String)message.get("sender_ip_address");
                int portNum = (int)message.get("sender_port_num");
                JSONObject activity =(JSONObject)message.get("activity");
                //If it has been the latest message in the server side, the server will discard it
                if (Control.getInstance().checkAckQueue(timestamp, senderIp, portNum, con)){
                    String ackMsg = Command.createActivityAcknowledgemnt(timestamp, senderIp, portNum);
                    con.writeMsg(ackMsg);
                    Control.getInstance().updateAckQueue(timestamp, senderIp, portNum, con);
                    
                    Message newMsg = new Message(con,timestamp,activity);
                    Control.getInstance().addToAllClientMsgBufferQueue(newMsg);
            		closeConnection=false;
            		}
            }
        } catch (ParseException e) {
        	Command.createInvalidMessage("JSON parse error while parsing message");
        	closeConnection=true;
        } catch (ClassCastException e2){
            Command.createInvalidMessage("Invalid Content Type");
            closeConnection=true;
        }
	}
		
	public boolean getResponse() {
		return closeConnection;
	}

}
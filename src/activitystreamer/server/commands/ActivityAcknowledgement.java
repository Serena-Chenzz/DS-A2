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

public class ActivityAcknowledgement {
    private boolean closeConnection=false;
    private final Logger log = LogManager.getLogger();

    public ActivityAcknowledgement(Connection con, String msg) {
        JSONParser parser = new JSONParser();
        JSONObject message;
        try{
            message = (JSONObject) parser.parse(msg);
            long msgTimestamp = (long)message.get("timestamp");
            String msgSenderIp = (String)message.get("sender_ip_address");
            int msgPortNum = (int)message.get("sender_port_num");
            Control.getInstance().removeMessageFromBufferQueue(msgTimestamp, msgSenderIp, msgPortNum,con);
        }
        catch (ParseException e) {
            Command.createInvalidMessage("JSON parse error while parsing message");
            closeConnection=true;
        }
    }
    public boolean getResponse() {
        return closeConnection;
    }
}

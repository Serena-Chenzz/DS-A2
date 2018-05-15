package activitystreamer.server;
import org.json.simple.JSONObject;

public class Message {
    private long timestamp;
    private Connection clientConnection;
    private JSONObject message;
    
    //Create a message to be stored inside the server side
    public Message(Connection clientConnection, JSONObject activity){
        this.timestamp = System.currentTimeMillis();
        this.clientConnection = clientConnection;
        this.message = activity;       
    }
    
    public long getTimeStamp(){
        return this.timestamp;
    }
    
    public String getSenderIp(){
        return this.clientConnection.getSocket().getInetAddress().toString();
    }
    
    public int getPortNum(){
        return this.clientConnection.getSocket().getPort();
    }
    
    public JSONObject getActivity(){
        return this.message;
    }
    //Overwrite equals function
    public boolean equals(Message msg2){
        return (this.timestamp == msg2.timestamp) && 
                (this.clientConnection.getSocket().getInetAddress().equals(msg2.clientConnection.getSocket().getInetAddress())) &&
                (this.clientConnection.getSocket().getPort()== msg2.clientConnection.getSocket().getPort());
    }
    
    //This message will be transferred between servers
    @SuppressWarnings("unchecked")
    public JSONObject getTransferredMsg(){
        message.put("timestamp", this.timestamp);
        message.put("sender_ip_address", this.clientConnection.getSocket().getInetAddress());
        message.put("sender_port_num", this.clientConnection.getSocket().getPort());
        message.put("activity_content", this.message);
        return message;
    }

}

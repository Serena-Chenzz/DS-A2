package activitystreamer.server.commands;

import java.util.ArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.models.User;
import activitystreamer.server.Control;


public class UserListReview {
	
    private final static Logger log = LogManager.getLogger();
	
	public UserListReview(ArrayList<User> localUserList) {
		
		try {//Check for every user coming from the userList message if:
			for(User user : localUserList) { //If user does not exist in the list, then do the following
				if(!Control.getLocalUserList().contains(user)) { //If user exists in current localUserList, then:
					if(Control.getInstance().checkLocalUser(user.getUsername())) {
						//Check if has same secret, add it to newLocalUserList
						if(Control.getInstance().checkLocalUserAndSecret(user.getUsername(), user.getSecret())) {
							//Keep the user, do nothing
						}else { //If user has different secret, then delete it from localUserList
							Control.getInstance().deleteLocalUser(user.getUsername(), user.getSecret());
						}
					}else { //If user does not exist, or user was previously created, then add it to new userList
						Control.getInstance().addLocalUser(user.getUsername(), user.getSecret());
					}
				}
			}
		}catch(Exception e) {
			log.error(e);
		}
	}

}

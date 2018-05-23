package activitystreamer.server.commands;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.models.User;
import activitystreamer.server.Control;


public class UserListReview {
	
    private final static Logger log = LogManager.getLogger();
    private ArrayList<User> newLocalUserList, currentLocalList;
	
	public UserListReview(ArrayList<User> localUserList) {
		newLocalUserList = new ArrayList<User>();
		currentLocalList = Control.getLocalUserList();
		
		try{
			//Clear localUserList to avoid dupl
			//Control.setLocalUserList(newLocalUserList);
			newLocalUserList.clear();
			//Check for every user coming from the userList message if:

			for(User user : localUserList) {
				//If user does not exist in the list, then do the following
				if(!Control.getLocalUserList().contains(user)) {
					//If user exists in current localUserList, then:
					if(Control.getInstance().checkLocalUser(user.getUsername())) {
						//Check if has same secret, add it to newLocalUserList
						if(Control.getInstance().checkLocalUserAndSecret(user.getUsername(), user.getSecret())) {
							System.out.println("nerdaa que paso: "+user);
							newLocalUserList.add(user);
						//If user has different secret, then delete it from localUserList
						}else {
							Control.getInstance().deleteLocalUser(user.getUsername(), user.getSecret());
						}
					//If user does not exist, or user was previously created, then add it to new userList	
					}else {
						newLocalUserList.add(user);
					}
				}
			}

			//If there was a new user registered in the network before network fail, add it to the new list too
			//even if it is not in the userList message coming from the reconnected server
			for(User user : Control.getLocalUserList()) {
				if(!newLocalUserList.contains(user)) {
					newLocalUserList.add(user);
				}
			}
			
			//Delete duplicate users in localUserList before sending
			for(int i = 0; i < newLocalUserList.size(); i++) {
	            for(int j = i + 1; j < newLocalUserList.size(); j++) {
	                if(newLocalUserList.get(i).equals(newLocalUserList.get(j))){
	                	newLocalUserList.remove(j);
	                    j--;
	                }
	            }
	        }
			
			//Finally, replace currentLocalUserList for a new one
			if(newLocalUserList.size()>0) {
				Control.setLocalUserList(newLocalUserList);
			}
		}catch(Exception e) {
			log.error(e);
		}
	}

}

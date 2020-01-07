/**
 * 
 */
package org.bgu.ise.ddb.history;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * 
 *
 */
@RestController
@RequestMapping(value = "/history")
public class HistoryController extends ParentController{



	/**
	 * The function inserts to the system storage triple(s)(username, title, timestamp). 
	 * The timestamp - in ms since 1970
	 * Advice: better to insert the history into two structures( tables) in order to extract it fast one with the key - username, another with the key - title
	 * @param username
	 * @param title
	 * @param response
	 */
	@RequestMapping(value = "insert_to_history", method={RequestMethod.GET})
	public void insertToHistory (@RequestParam("username")    String username,
			@RequestParam("title")   String title,
			HttpServletResponse response){
		boolean inserted=false;
		System.out.println(username+" "+title);
		//:TODO your implementation
		try {
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			DBCollection  dbCollection = mongoClient.getDB("projectDB").getCollection("UsersHistory");

			long timestamp =  new Date().getTime();
			BasicDBObject userHistory = new BasicDBObject();
			userHistory.put("UserName", username);
			userHistory.put("Title", title);
			userHistory.put("Timestamp", timestamp );

			if(isUserExist(username) &&isExistsItem(title))
			{
				dbCollection.insert(userHistory);
				inserted=true;
			}
			
			mongoClient.close();
			
			HttpStatus status;
			if(inserted)
				status = HttpStatus.OK;
			else
				status = HttpStatus.CONFLICT;
			response.setStatus(status.value());

		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}

		HttpStatus status;
		if(inserted)
			status = HttpStatus.OK;
		else
			status = HttpStatus.CONFLICT;
		response.setStatus(status.value());
	}



	/**
	 * The function retrieves  users' history
	 * The function return array of pairs <title,viewtime> sorted by VIEWTIME in descending order
	 * @param username
	 * @return
	 */
	@RequestMapping(value = "get_history_by_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByUser(@RequestParam("entity")    String username){
		//:TODO your implementation
		ArrayList<HistoryPair> userHistoryPairList = new ArrayList<HistoryPair>();
		try {
			MongoClient mongoClient = null;
			mongoClient = new MongoClient( "localhost" , 27017 );
			DBCollection  dbCollection = mongoClient.getDB("projectDB").getCollection("UsersHistory");
			BasicDBObject queryResult = new BasicDBObject();
			queryResult.put("UserName", username);
			DBCursor dbCursor = dbCollection.find(queryResult).sort(new BasicDBObject("Timestamp",-1));

			while (dbCursor.hasNext()) 
			{ 
				DBObject theObj = dbCursor.next();
				String credentials = (String) theObj.get("Title");
				long timestamp = (long) theObj.get("Timestamp");
				userHistoryPairList.add(new HistoryPair(credentials,new Date(timestamp)));

			}
			mongoClient.close();
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}

		return userHistoryPairList.toArray(new HistoryPair[userHistoryPairList.size()]);
	}


	/**
	 * The function retrieves  items' history
	 * The function return array of pairs <username,viewtime> sorted by VIEWTIME in descending order
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_history_by_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByItems(@RequestParam("entity")    String title){
		//:TODO your implementation
		ArrayList<HistoryPair> userHistoryPairList = new ArrayList<HistoryPair>();
		try {
			MongoClient mongoClient = null;
			mongoClient = new MongoClient( "localhost" , 27017 );
			DBCollection  dbCollection = mongoClient.getDB("projectDB").getCollection("UsersHistory");
			BasicDBObject queryResult = new BasicDBObject();
			queryResult.put("Title", title);
			DBCursor dbCursor = dbCollection.find(queryResult).sort(new BasicDBObject("Timestamp",-1));

			while (dbCursor.hasNext()) 
			{ 
				DBObject theObj = dbCursor.next();
				String credentials = (String) theObj.get("UserName");
				long timestamp = (long) theObj.get("Timestamp");
				userHistoryPairList.add(new HistoryPair(credentials,new Date(timestamp)));

			}
			mongoClient.close();
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}

		
		return userHistoryPairList.toArray(new HistoryPair[userHistoryPairList.size()]);
	}

	/**
	 * The function retrieves all the  users that have viewed the given item
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_users_by_item",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  User[] getUsersByItem(@RequestParam("title") String title){
		//:TODO your implementation
		ArrayList<User> users = new ArrayList<User>();
		try {
			HistoryPair[] usersHistoryPairList = getHistoryByItems(title);
			MongoClient mongoClient = null;
			mongoClient = new MongoClient( "localhost" , 27017 );
			DBCollection  dbCollection;
			BasicDBObject queryResult;

			for (HistoryPair usersHistoryPair : usersHistoryPairList) {
				//				mongoClient = new MongoClient( "localhost" , 27017 );
				dbCollection = mongoClient.getDB("projectDB").getCollection("Users");
				queryResult = new BasicDBObject();
				queryResult.put("UserName", usersHistoryPair.credentials);
				DBCursor dbCursor = dbCollection.find(queryResult);
				if (dbCursor.hasNext()) 
				{ 
					DBObject theObj = dbCursor.next();
					String userName = (String) theObj.get("UserName");
					String firstName = (String) theObj.get("FirstName");
					String lastName = (String) theObj.get("LastName");
					users.add(new User(userName, firstName, lastName));
				}
				mongoClient.close();
			}

		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}

		return users.toArray(new User[users.size()]);
	}

	/**
	 * The function calculates the similarity score using Jaccard similarity function:
	 *  sim(i,j) = |U(i) intersection U(j)|/|U(i) union U(j)|,
	 *  where U(i) is the set of usernames which exist in the history of the item i.
	 * @param title1
	 * @param title2
	 * @return
	 */
	@RequestMapping(value = "get_items_similarity",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	public double  getItemsSimilarity(@RequestParam("title1") String title1,
			@RequestParam("title2") String title2){
		//:TODO your implementation
		double Similarity=0;
		try {

			Set<String> title1UsersList = ToUserNameList(getHistoryByItems(title1));
			Set<String> title2UsersList = ToUserNameList(getHistoryByItems(title2));

			Set<String> unionList=  new HashSet<String>(title1UsersList);
			unionList.addAll(title2UsersList);

			Set<String> intersectionList=  new HashSet<String>(title1UsersList);
			intersectionList.retainAll(title2UsersList);

			if(unionList.size() ==0 ) {
				return Similarity;
			}

			Similarity = ((double)intersectionList.size())/unionList.size();

		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}

		return Similarity;
	}


	private Set<String> ToUserNameList(HistoryPair[] titleHistoryPairList) {
		Set<String> titleUsersList = new HashSet<String>();

		for (HistoryPair historyPair : titleHistoryPairList) {
			titleUsersList.add(historyPair.credentials);

		}	
		return titleUsersList;
	}

	private boolean isUserExist( String username){
		System.out.println(username);
		boolean result = false;
		//:TODO your implementation
		try {
			MongoClient mongoClient = null;
			mongoClient = new MongoClient( "localhost" , 27017 );
			DBCollection  dbCollection = mongoClient.getDB("projectDB").getCollection("Users");
			BasicDBObject queryResult = new BasicDBObject();
			queryResult.put("UserName", username);
			DBCursor dbCursor = dbCollection.find(queryResult);
			while (dbCursor.hasNext()) 
			{ 
				result = true;
				dbCursor.next();
			}
			mongoClient.close();
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}
		return result;

	}


	private boolean isExistsItem(String title) {
		System.out.println(title);
		boolean result = false;
		//:TODO your implementation
		try {
			MongoClient mongoClient = null;
			mongoClient = new MongoClient( "localhost" , 27017 );
			DBCollection  dbCollection = mongoClient.getDB("projectDB").getCollection("MediaItems");
			BasicDBObject queryResult = new BasicDBObject();
			queryResult.put("Title", title);
			DBCursor dbCursor = dbCollection.find(queryResult);
			while (dbCursor.hasNext()) 
			{ 
				result = true;
				dbCursor.next();
			}
			mongoClient.close();
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}
		return result;

	}
}

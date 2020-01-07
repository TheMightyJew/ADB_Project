/**
 * 
 */
package org.bgu.ise.ddb.registration;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

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
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController{


	/**
	 * The function checks if the username exist,
	 * in case of positive answer HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT,
	 * else insert the user to the system  and set to HttpStatus in HttpServletResponse HttpStatus.OK
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 */
	@RequestMapping(value = "register_new_customer", method={RequestMethod.POST})
	public void registerNewUser(@RequestParam("username") String username,
			@RequestParam("password")    String password,
			@RequestParam("firstName")   String firstName,
			@RequestParam("lastName")  String lastName,
			HttpServletResponse response){
		System.out.println(username+" "+password+" "+lastName+" "+firstName);

		System.out.println("registerNewUser");
		//:TODO your implementation
		try {
			if (isExistUser(username)) {
				HttpStatus status = HttpStatus.CONFLICT;
				response.setStatus(status.value());
			}else {
				MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
				DBCollection  dbCollection = mongoClient.getDB("projectDB").getCollection("Users");

				BasicDBObject user = new BasicDBObject();
				user.put("UserName", username);
				user.put("FirstName", firstName);
				user.put("LastName", lastName);
				user.put("Password", password);
				user.put("RegistrationDate", new Date());

				dbCollection.insert(user);
				mongoClient.close();

				HttpStatus status = HttpStatus.OK;
				response.setStatus(status.value());
			}
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}

	}

	/**
	 * The function returns true if the received username exist in the system otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_user", method={RequestMethod.GET})
	public boolean isExistUser(@RequestParam("username") String username) throws IOException{
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

	/**
	 * The function returns true if the received username and password match a system storage entry, otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "validate_user", method={RequestMethod.POST})
	public boolean validateUser(@RequestParam("username") String username,
			@RequestParam("password")    String password) throws IOException{
		System.out.println(username+" "+password);
		boolean result = false;
		//:TODO your implementation
		try {
			MongoClient mongoClient = null;
			mongoClient = new MongoClient( "localhost" , 27017 );
			DBCollection  dbCollection = mongoClient.getDB("projectDB").getCollection("Users");
			BasicDBObject queryResult = new BasicDBObject();
			queryResult.put("UserName", username);
			queryResult.put("Password", password);
			DBCursor dbCursor = dbCollection.find(queryResult);
			while (dbCursor.hasNext()) 
			{ 
				result = true;
				dbCursor.next();
			}
			mongoClient.close();
		}catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}



		return result;

	}

	/**
	 * The function retrieves number of the registered users in the past n days
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "get_number_of_registred_users", method={RequestMethod.GET})
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException{
		System.out.println(days+"");
		int result = 0;
		//:TODO your implementation
		try {
			MongoClient mongoClient = null;
			mongoClient = new MongoClient( "localhost" , 27017 );
			DBCollection  dbCollection = mongoClient.getDB("projectDB").getCollection("Users");
			DBCursor dbCursor = dbCollection.find();
			Date now= new Date();
			Date targetDate = new Date( now.getTime() - days * 24 * 3600 * 1000l );
			System.out.println("targetDate"+targetDate.toString());
			while (dbCursor.hasNext()) 
			{ 
				DBObject theObj = dbCursor.next();
				Date registrationDate = (Date) theObj.get("RegistrationDate");
				if( registrationDate.getTime()> targetDate.getTime()) {
					result++;
				}
				
			}
			mongoClient.close();
		}catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}


		return result;

	}

	/**
	 * The function retrieves all the users
	 * @return
	 */
	@RequestMapping(value = "get_all_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(User.class)
	public  User[] getAllUsers(){
		//:TODO your implementation
		ArrayList<User> users = new ArrayList<User>();
		try {
			MongoClient mongoClient = null;
			mongoClient = new MongoClient( "localhost" , 27017 );
			DBCollection  dbCollection = mongoClient.getDB("projectDB").getCollection("Users");

			DBCursor dbCursor = dbCollection.find();

			while (dbCursor.hasNext()) 
			{ 
				DBObject theObj = dbCursor.next();
				String userName = (String) theObj.get("UserName");
				String firstName = (String) theObj.get("FirstName");
				String lastName = (String) theObj.get("LastName");
				String password = (String) theObj.get("Password");
				users.add(new User(userName, password ,firstName, lastName));

			}
			mongoClient.close();
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}

		return users.toArray(new User[users.size()]);
	}

}

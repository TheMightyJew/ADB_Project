/**
 * 
 */
package org.bgu.ise.ddb.items;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
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
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {

	private Connection connection =null;
	private String username="****";
	private String password="abcd";
	private String connectionUrl="jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/oracle";
	private final String driver="oracle.jdbc.driver.OracleDriver";

	private void ConnectToOracleDB()
	{
		try 
		{
			Class.forName(this.driver); //registration of the driver
			this.connection = DriverManager.getConnection(this.connectionUrl, this.username, this.password);
			this.connection.setAutoCommit(false);
		}catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * The function copy all the items(title and production year) from the Oracle table MediaItems to the System storage.
	 * The Oracle table and data should be used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method={RequestMethod.GET})
	public void fillMediaItems(HttpServletResponse response){
		System.out.println("was here");
		//:TODO your implementation
		ResultSet rs =null;
		PreparedStatement ps =null;
		List<MediaItems> mediaItems = new ArrayList<MediaItems>();
		try {
			ConnectToOracleDB();
			String query = "SELECT title,prod_year FROM MediaItems";
			ps = connection.prepareStatement(query);
			rs = ps.executeQuery();

			while (rs.next()) {
				mediaItems.add(new MediaItems(rs.getString(1), rs.getInt(2)));
			}
			rs.close();
		} catch (Exception e) {
			// TODO: handle exception
		}finally {
			try {
				if (ps != null) {
					ps.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			try {
				if (connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		for (MediaItems mediaItem : mediaItems) {
			if(!isItemExists(mediaItem.getTitle()))
			{
				AddMovieToMongoDB(mediaItem);
			}
		}


		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}



	private boolean isItemExists(String title) {
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
	

	/**
	 * The function copy all the items from the remote file,
	 * the remote file have the same structure as the films file from the previous assignment.
	 * You can assume that the address protocol is http
	 * @throws IOException 
	 */
	@RequestMapping(value = "fill_media_items_from_url", method={RequestMethod.GET})
	public void fillMediaItemsFromUrl(@RequestParam("url")    String urladdress,
			HttpServletResponse response) throws IOException{
		System.out.println(urladdress);

		//:TODO your implementation

		URL url = new URL(urladdress);
		BufferedReader br = null;
		String line = "";

		try {
			br = new BufferedReader(new BufferedReader(new InputStreamReader(url.openStream())));
			while ((line = br.readLine()) != null) {
				String[] tuple = line.split(",");
				System.out.println(line);
				String movieTitle= tuple[0];
				String movieYearString=tuple[1];
				try {
					int movieYear = Integer.parseInt(movieYearString);
					if(!isItemExists(movieTitle))
					{
						AddMovieToMongoDB(new MediaItems(movieTitle,movieYear));
					}
					
				} catch (Exception e) {
					// TODO: handle exception
					System.out.println(e);
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}



		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}


	private void AddMovieToMongoDB(MediaItems mediaItem) {
		// TODO Auto-generated method stub
		try {
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			DBCollection  dbCollection = mongoClient.getDB("projectDB").getCollection("MediaItems");

			BasicDBObject item = new BasicDBObject();
			item.put("Title", mediaItem.getTitle());
			item.put("Year", mediaItem.getProdYear());

			dbCollection.insert(item);
			mongoClient.close();
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}
	}



	/**
	 * The function retrieves from the system storage N items,
	 * order is not important( any N items) 
	 * @param topN - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public  MediaItems[] getTopNItems(@RequestParam("topn")    int topN){
		//:TODO your implementation
		ArrayList<MediaItems> mediaItemsList = new ArrayList<MediaItems>();
		if(topN < 1) {

			return new MediaItems[0];
		}
		try {
			MongoClient mongoClient = null;
			mongoClient = new MongoClient( "localhost" , 27017 );
			DBCollection  dbCollection = mongoClient.getDB("projectDB").getCollection("MediaItems");

			DBCursor dbCursor = dbCollection.find().limit(topN);

			while (dbCursor.hasNext()) 
			{ 
				DBObject theObj = dbCursor.next();
				String title = (String) theObj.get("Title");
				int year = (int) theObj.get("Year");
				mediaItemsList.add(new MediaItems(title ,year));

			}
			mongoClient.close();
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}

		return mediaItemsList.toArray(new MediaItems[mediaItemsList.size()]);
	}


}

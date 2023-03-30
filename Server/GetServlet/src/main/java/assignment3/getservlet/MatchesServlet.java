package assignment3.getservlet;

import assignment3.config.constant.LoadTestConfig;
import assignment3.config.constant.MongoConnectionInfo;
import assignment3.config.datamodel.SwipeDetails;
import assignment3.config.datamodel.Matches;
import assignment3.config.datamodel.ResponseMsg;
import assignment3.config.util.Pair;
import com.google.gson.Gson;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.bson.Document;
import static com.mongodb.client.model.Filters.eq;

@WebServlet(name = "assignment3.getservlet.MatchesServlet", value = "/matches")
public class MatchesServlet extends HttpServlet {
/** TODO: Use Dao to connect and query from DB?
 * TODO: Does this Servlet need to be multi-threaded? Do we also need to create a DB connection pool,
 * for Tomcat's multi-threads to use? like we did for RMQ(SwipeServlet, send msg to RMQ)
 **/
  private MongoClient mongoClient;
  private final static Class<? extends List> listDocClazz = new ArrayList<Document>().getClass();

  private final static int MAX_MATCH_SIZE = 100;

  @Override
  public void init() throws ServletException {
    super.init();
    ConnectionString mongoUri = new ConnectionString(MongoConnectionInfo.uri);

    MongoClientSettings settings = MongoClientSettings.builder()
        .applyConnectionString(mongoUri)
        .applyToConnectionPoolSettings(builder ->
            builder
                .maxConnectionIdleTime(60, TimeUnit.SECONDS)
                .maxSize(LoadTestConfig.MATCHES_SERVLET_DB_MAX_CONNECTION)
                .maxWaitTime(10, TimeUnit.SECONDS))
        .build(); // TODO: Extract common code (for GetServlet and Consumer) to Config package.

    try {
      this.mongoClient = MongoClients.create(settings);
    } catch (MongoException me) {
      System.err.println("Cannot create MongoClient for MatchesServlet: " + me);
    }
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");
    ResponseMsg responseMsg = new ResponseMsg();
    Gson gson = new Gson();

    String urlPath = request.getPathInfo();

    // check we have a URL!
    if (urlPath == null || urlPath.isEmpty()) {
      responseMsg.setMessage("missing path parameter: userID");
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      response.getOutputStream().print(gson.toJson(responseMsg));
      response.getOutputStream().flush();
      return;
    }

    // check if URL path HAS exactly one parameter (userId) and
    // userId has a valid value: is integer and within range
    Pair urlValidationRes = this.isUrlValid(urlPath);
    if (!urlValidationRes.isUrlPathValid()) {
      responseMsg.setMessage("invalid path parameter: should be a positive integer <= " + Math.max(SwipeDetails.MAX_SWIPEE_ID, SwipeDetails.MAX_SWIPER_ID));
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST); // Invalid inputs
      response.getOutputStream().print(gson.toJson(responseMsg));
      response.getOutputStream().flush();
      return;
    }

    Integer swiperId = Integer.valueOf(urlValidationRes.getParam());


    // TODO: HardCoded for now. Need to fetch matchList from DB
//    List<String> matches = Arrays.asList("123", "456", "789"); //new ArrayList<>({"123", "456", "789"});
//    Matches matchList = new Matches(matches);
//    response.setStatus(HttpServletResponse.SC_OK);
//    response.getOutputStream().print(gson.toJson(matchList));
//    response.getOutputStream().flush();

    // Connect to MongoDB
    MongoDatabase database = this.mongoClient.getDatabase(MongoConnectionInfo.DATABASE);
    MongoCollection<Document> matchesCollection = database.getCollection(MongoConnectionInfo.MATCH_COLLECTION);
    this.readMatchesCollection(matchesCollection, swiperId, gson, responseMsg, response);
  }

  private void readMatchesCollection(MongoCollection<Document> collection, Integer swiperId, Gson gson, ResponseMsg responseMsg, HttpServletResponse response)
      throws IOException {
    Document doc = collection.find(eq("_id", swiperId)).first();

    if (doc == null) {  // <--> No document matches the _id
      responseMsg.setMessage("User Not Found");
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      response.getOutputStream().print(gson.toJson(responseMsg));
      response.getOutputStream().flush();
      System.out.println("Respond to Client: User Not Found:" + swiperId);
    } else {
      List<Integer> matchesList = doc.get("matches", listDocClazz);
      if (matchesList.size() > MAX_MATCH_SIZE) {
        matchesList = matchesList.subList(0, MAX_MATCH_SIZE);
      }
      //Convert List<Integer> to List<String>
      Matches matches = new Matches(matchesList.stream().map(id -> String.valueOf(id)).collect(
          Collectors.toList()));
      responseMsg.setMessage("Fetched Match!");
      response.setStatus(HttpServletResponse.SC_OK);
      response.getWriter().print(gson.toJson(matches));
      response.getWriter().flush();
      System.out.println("Respond to Client: Fetched Match for:" + swiperId);

    }

  }

  private Pair isUrlValid(String urlPath) {
    /**
     * Check if url path has exactly one param: {userId} and its valid(within  range)
     */
    String[] urlParts = urlPath.split("/");

    if (urlParts.length != 2) {
      System.out.print("not 2");
      return new Pair(false, null);
    }

    int userId;
    try {
      userId = Integer.valueOf(urlParts[1]);
      return new Pair(userId <= Math.max(SwipeDetails.MAX_SWIPEE_ID, SwipeDetails.MAX_SWIPER_ID) && userId >= 1,
          urlParts[1]);
    } catch (NumberFormatException e) {
      return new Pair(false, null);
    }

  }

}

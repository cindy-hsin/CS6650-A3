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
public class MatchesServlet extends AbstractGetServlet {
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

    MongoClientSettings settings = MongoConnectionInfo.buildMongoSettings("Matches");
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
    Integer swiperId = this.validateAndExtractId(request, response, responseMsg, gson);

    if (swiperId == null) {
      return;
    }
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
      responseMsg.setMessage("Fetched Matches for userId " + swiperId + "!");
      response.setStatus(HttpServletResponse.SC_OK);
      response.getWriter().print(gson.toJson(matches));
      response.getWriter().flush();
      System.out.println("MatchesServlet Respond to Client: Fetched for:" + swiperId);

    }

  }


}

package assignment3.getservlet;

import static com.mongodb.client.model.Filters.eq;

import assignment3.config.constant.MongoConnectionInfo;
import assignment3.config.datamodel.MatchStats;
import assignment3.config.datamodel.ResponseMsg;
import com.google.gson.Gson;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.bson.Document;

@WebServlet(name = "assignment3.getservlet.StatsServlet", value = "/stats")
public class StatsServlet extends AbstractGetServlet {
  private MongoClient mongoClient;
  @Override
  public void init() throws ServletException {
    super.init();
    MongoClientSettings settings = MongoConnectionInfo.buildMongoSettings("Stats");
    try {
      this.mongoClient = MongoClients.create(settings);
    } catch (MongoException me) {
      System.err.println("Cannot create MongoClient for StatsServlet: " + me);
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

    // Connect to MongoDB
    MongoDatabase database = this.mongoClient.getDatabase(MongoConnectionInfo.DATABASE);
    MongoCollection<Document> statsCollection = database.getCollection(MongoConnectionInfo.STATS_COLLECTION);

    this.readStatsCollection(statsCollection, swiperId, gson, responseMsg, response);
  }

  private void readStatsCollection(MongoCollection<Document> collection, Integer swiperId, Gson gson, ResponseMsg responseMsg, HttpServletResponse response)
      throws IOException {
    Document doc = collection.find(eq("_id", swiperId)).first();

    if (doc == null) {  // <--> No document matches the _id
      responseMsg.setMessage("User Not Found");
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      response.getOutputStream().print(gson.toJson(responseMsg));
      response.getOutputStream().flush();
      System.out.println("Respond to Client: User Not Found:" + swiperId);
    } else {
      int likes = doc.getInteger("likes");
      int dislikes = doc.getInteger("dislikes");
      MatchStats stats = new MatchStats().numLikes(likes).numDislikes(dislikes);
      responseMsg.setMessage("Fetched Stats for userId " + swiperId + "!");
      response.setStatus(HttpServletResponse.SC_OK);
      response.getWriter().print(gson.toJson(stats));
      response.getWriter().flush();
      System.out.println("StatsServlet Respond to Client: Fetched for:" + swiperId);

    }

  }




}

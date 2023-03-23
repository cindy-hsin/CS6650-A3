package assignment3.getservlet;

import assignment3.config.datamodel.SwipeDetails;
import assignment3.servlet.datamodel.Matches;
import assignment3.servlet.datamodel.ResponseMsg;
import assignment3.servlet.util.Pair;
import com.google.gson.Gson;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@WebServlet(name = "assignment3.getservlet.MatchesServlet", value = "/matches")
public class MatchesServlet extends HttpServlet {
/** TODO: Use Dao to connect and query from DB?
 * TODO: Does this Servlet need to be multi-threaded? Do we also need to create a DB connection pool,
 * for Tomcat's multi-threads to use? like we did for RMQ(SwipeServlet, send msg to RMQ)
 **/
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    response.setContentType("application/json");
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

    String userId = urlValidationRes.getParam();

    // TODO: Connect and Query DB with userId.
    //  If any Exception is thrown when connecting and querying DB -> catch Exception, response.setStatus('500');
    //  If userId doesn't have a match record in DB: '400' response;
    //  else, return '200' as status code & the matchList (minItems:1, maxItems: 10) as response body
    //  (need to 1. Construct a Matches object that represents {"matchList": [xx,yy,zz...]}, ->
    //  -> Control Accept header?? (See Swagger API definition)
    //  2. convert the Object into json, using gson.toJson())


    // TODO: If userId doesn't have a match -> return '400';
    // else:

    List<String> matches = Arrays.asList("123", "456", "789"); //new ArrayList<>({"123", "456", "789"});
    Matches matchList = new Matches(matches);   // TODO: HardCoded for now. Need to fetch matchList from DB

    responseMsg.setMessage(gson.toJson(matchList));
    response.setStatus(HttpServletResponse.SC_OK);
    response.getOutputStream().print(gson.toJson(responseMsg));
    response.getOutputStream().flush();

  }

  private Pair isUrlValid(String urlPath) {
    /**
     * Check if url path has exactly one param: {userId} and its valid(within  range)
     */
    // TODO: Confirm: userId < 1000000? max(swiperId, swipeeId)?
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

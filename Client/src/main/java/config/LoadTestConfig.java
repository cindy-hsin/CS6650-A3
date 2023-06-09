package config;

public class LoadTestConfig {
  public static final int NUM_TOTAL_REQUESTS = 500000;
  public static final int NUM_THREADS = 200;    // Change this value for experiment

  public static final int POST_SUCCESS_CODE = 201;

  public static final int GET_SUCCESS_CODE = 200;

  public static final int MAX_RETRY = 5;


  // HTTP servlets:
  // remote: "http://xxxx:8080/A1-Server_war";
  // local: "http://localhost:8080/A1_Server_war_exploded"

  // SpringBoot server:
  // remote: http://xxxxx:8080/A1-SpringBootServer
  // local: http://localhost:8080/A1-SpringBootServer_war


  public static final String SWIPE_URL = "http://34.212.214.202:8080/PostServlet_war"; // "http://localhost:8080/PostServlet_war_exploded";
  public static final String GET_URL = "http://35.91.149.233:8080/GetServlet_war"; // "http://localhost:8080/GetServlet_war_exploded";


}
//34.208.113.22
// A2-AppLoadBalancer-1646955486.us-west-2.elb.amazonaws.com


// service:jmx:rmi://ec2-54-218-125-29.us-west-2.compute.amazonaws.com:10002/jndi/rmi://ec2-54-218-125-29.us-west-2.compute.amazonaws.com:10001/jmxrmi
//service:jmx:rmi://ec2-54-218-251-163.us-west-2.compute.amazonaws.com:10002/jndi/rmi://ec2-54-218-251-163.us-west-2.compute.amazonaws.com:10001/jmxrmi
//service:jmx:rmi://ec2-54-188-20-166.us-west-2.compute.amazonaws.com:10002/jndi/rmi://ec2-54-188-20-166.us-west-2.compute.amazonaws.com:10001/jmxrmi

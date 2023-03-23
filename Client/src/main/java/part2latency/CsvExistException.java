package part2latency;

public class CsvExistException extends Exception{

  public CsvExistException(String csvFileName) {
    super("Previous records: " + csvFileName + " already exists! Please delete the csv and re-run the program");
  }
}

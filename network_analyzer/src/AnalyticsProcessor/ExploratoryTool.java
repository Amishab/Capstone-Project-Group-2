package AnalyticsProcessor;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;

import utils.Log;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ExploratoryTool {
    private final Driver neo4jDriver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "1234"));
    QueryBuilder qb;
    private Log log;

    private ExploratoryTool() throws FileNotFoundException {

        this.log = new Log();
        log.add(new File(new File("c://temp//"),"exploratory_log.txt"));

        this.qb = new QueryBuilder();

        try (Session session = neo4jDriver.session()) {
            session.run("MATCH (n) RETURN n LIMIT 1");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void run_queries(){

        Session session = neo4jDriver.session();
        List<Object> args =  new ArrayList<>();

        args.add("health");
        String query = qb.buildquery("get_inferred_relations_for_news_containing_word",args);

        log.info("Query to get inferred relations from sentence containing :"+args.toString());
        log.info(qb.result_as_tab_string(session.run(query)));

    }



    public static void main(String args[]) throws IOException {

        ExploratoryTool et = new ExploratoryTool();
        et.run_queries();


    }

}



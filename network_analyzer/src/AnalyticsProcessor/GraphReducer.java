package AnalyticsProcessor;

import java.util.ArrayList;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.*;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class GraphReducer {

    ArrayList<Senator>  senators;
    List<String> sen_names;
    List<String> sen_last_names;
    Session session;
    String senators_csv = "data/senate_w_LN.csv";
    private Driver neo4jDriver;


    public GraphReducer(Driver nd) throws IOException {
        this.neo4jDriver = nd;
        this.senators = new ArrayList<>();
        this.sen_names  = new ArrayList<String>();
        this.sen_last_names  = new ArrayList<String>();
        this.readCSV(senators_csv);
        for(Senator sen:this.senators){
            sen_names.add(sen.name);
            sen_last_names.add(sen.lastName);
        }

        System.out.println("Initialized Graph Reducer");
    }


    private class Senator {
        String name;
        String lastName;
        int y_i_o;
        String party;
        String state;
        int termEnds;


        Senator(String sname,String slastName,int sy_i_o,String sparty,String sstate,int stermEnds){
            name=sname;
            lastName = slastName;
            y_i_o = sy_i_o;
            party = sparty;
            state = sstate;
            termEnds = stermEnds;

        }

    }

    private void readCSV(String filePath) throws IOException{
        try(
                Reader reader = Files.newBufferedReader(Paths.get(filePath));
                CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build()
                ){
            String[] st;
            while ((st = csvReader.readNext()) != null) {

                this.senators.add(new Senator(st[0],st[1],Integer.parseInt(st[2]),st[3],st[4],Integer.parseInt(st[5])));
            }
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void runRules(){


        coalesce_compound_edges();
        find_NER_discard_unwanted();
        traverse_path_connecting_ners();
    }

    public void coalesce_compound_edges(){
        StatementResult result;
        String cname;
        try(Session session = this.neo4jDriver.session()){

            result = session.run("MATCH (a:PERSON)-[r:dep]->(b:PERSON) " +
                                    "WHERE ABS(a.idx-b.idx)=1 AND r.type='compound' AND NOT(b)-[]->()  " +
                                    "RETURN " +
                                                "a.word as aword," +"a.idx as aidx,"+
                                                "b.word as bword," +"b.idx as bidx");

            while (result.hasNext()){

                Record record = result.next();
                //System.out.println("coalescing "+record.get("aword" ).asString()+" AND " +record.get("bword"));

                if (record.get("aidx").asInt()>record.get("bidx").asInt()){
                    cname = record.get("bword").asString()+" "+record.get("aword").asString();
                }else{
                    cname = record.get("aword").asString()+" "+record.get("bword").asString();
                }

                session.run("MATCH (a:PERSON)-[r:dep]->(b:PERSON) " +
                               "WHERE a.idx="+record.get("aidx").asInt()+" AND b.idx="+record.get("bidx").asInt()+" "+
                               "WITH a,b,r "+
                               "SET a.word =\""+cname+"\" "+
                               "DELETE r,b");

                System.out.println("coalesced "+record.get("aword" ).asString()+" AND " +record.get("bword").asString());


            }
        }
    }

    public void find_NER_discard_unwanted(){

        StatementResult result;
        try (Session session = this.neo4jDriver.session()) {
            result=session.run("MATCH (a:PERSON) RETURN a.word as aword,a.idx as aidx");

            while (result.hasNext()){

                Record record = result.next();
                String pname = record.get("aword").asString();
                known_entity_search_result ke = is_this_named_entity_needed(pname);
                if (!ke.is_needed){
                    System.out.println("find_NER_discard() : Discarding person : "+pname);
                    session.run("MATCH (a:PERSON)-[r:dep]-() "+
                                   "WHERE a.idx="+record.get("aidx").asInt()+" "+
                                   "WITH a,r "+
                                   "DELETE a,r ");


                }
                else {

                    session.run("MATCH (a:PERSON) " +
                                " WHERE a.idx="+record.get("aidx").asInt()+" " +
                                " SET a.word = \""+this.sen_names.get(ke.found_at)+"\" " );

                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }

    public class known_entity_search_result {
        boolean is_needed;
        int found_at;

        public known_entity_search_result(boolean b, int i){
            is_needed = b;
            found_at =i;
        }

    }
    public known_entity_search_result is_this_named_entity_needed(String nst){

        int index =0;
        known_entity_search_result ret;

        for (String sen:this.sen_names){
            if (sen.equalsIgnoreCase(nst)){
                ret=new known_entity_search_result(true,index);
                return ret;
            }
            index++;
        }

        index=0;

        for (String lsen:this.sen_last_names){
            if (lsen.equalsIgnoreCase(nst)){
                ret=new known_entity_search_result(true,index);
                return ret;
            }
            index++;
        }

        return (new known_entity_search_result(false,0));
    }

    public void traverse_path_connecting_ners(){
        StatementResult result;
        try (Session session = this.neo4jDriver.session()) {
            result = session.run("MATCH (a:PERSON) RETURN a.word as aword,a.idx as aidx");

        }


    }

}

package AnalyticsProcessor;

import java.util.*;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.types.Node;
import org.neo4j.driver.v1.types.Path;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;

public class GraphReducer {

    ArrayList<Senator>  senators;
    List<String> sen_names;
    List<String> sen_last_names;
    HashMap sen_alias;
    Session session;
    String senators_csv = "data/senate_w_LN.csv";
    private Driver neo4jDriver;



    public GraphReducer(Driver nd) throws IOException {
        this.neo4jDriver = nd;
        this.senators = new ArrayList<>();
        this.sen_names  = new ArrayList<String>();
        this.sen_last_names  = new ArrayList<String>();
        this.sen_alias = new HashMap<String,Integer>();
        this.readCSV(senators_csv);
        for(Senator sen:this.senators){
            this.sen_names.add(sen.name);
            this.sen_last_names.add(sen.lastName);
            for (String als_name:sen.alias){
                System.out.println("alias name: "+als_name);
                this.sen_alias.put(als_name,(this.sen_names.size())-1);

            }
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
        ArrayList<String> alias;


        Senator(String sname,String slastName,int sy_i_o,String sparty,String sstate,int stermEnds){
            name=sname;
            lastName = slastName;
            y_i_o = sy_i_o;
            party = sparty;
            state = sstate;
            termEnds = stermEnds;
        }

        Senator(String sname,String slastName,int sy_i_o,String sparty,String sstate,int stermEnds,String als){
            name=sname;
            lastName = slastName;
            y_i_o = sy_i_o;
            party = sparty;
            state = sstate;
            termEnds = stermEnds;
            if (!als.isEmpty()){
                alias = new ArrayList<>(Arrays.asList(als.split(",",-1)));
            }
            else alias = new ArrayList<>();



        }

    }

    private void readCSV(String filePath) throws IOException{
        try(
                Reader reader = Files.newBufferedReader(Paths.get(filePath));
                CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(1).build()
                ){
            String[] st;
            while ((st = csvReader.readNext()) != null) {
                if (st.length==6){
                    this.senators.add(new Senator(st[0],st[1],Integer.parseInt(st[2]),st[3],st[4],Integer.parseInt(st[5])));
                }
                else if (st.length==7){
                    this.senators.add(new Senator(st[0],st[1],Integer.parseInt(st[2]),st[3],st[4],Integer.parseInt(st[5]),st[6]));
                }


            }
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void runRules(String docId,int sen_id){


        System.out.println("Processing docId: "+docId);
        coalesce_compound_edges(docId,sen_id);
        find_NER_and_unwanted(docId,sen_id);
        traverse_path_connecting_ners(docId,sen_id);
    }


    /**
     * Coalesce consecutive PERSON words connected by a 'compound' edge
     *
     * current rules :
     *     Find PERSON NODES -> that have word indices differed by 1 ( consecutive words in the sentence) ->  combine both words to construct full name
     *
     * todo :
     *      Test the functionality for three consecutive words. For example , how does this work for "Charles Ernest Grassley" ?
     */

    public void coalesce_compound_edges(String docId,int sen_id){
        StatementResult result;
        String cname;
        try(Session session = this.neo4jDriver.session()){

            String dId_sId="{docId:"+docId+" , senId:"+sen_id+"}";

            String conseq_person_query =
                    "MATCH (a:PERSON"+dId_sId+")-[r:dep]->(b:PERSON"+dId_sId+") " +
                    "WHERE ABS(a.idx-b.idx)=1 AND r.type='compound' AND NOT(b)-[]->()  " +
                    "RETURN " +
                    "a.word as aword," +"a.idx as aidx,"+
                    "b.word as bword," +"b.idx as bidx";

            result = session.run(conseq_person_query);

            while (result.hasNext()){

                Record record = result.next();
                //System.out.println("coalescing "+record.get("aword" ).asString()+" AND " +record.get("bword"));

                if (record.get("aidx").asInt()>record.get("bidx").asInt()){
                    cname = record.get("bword").asString()+" "+record.get("aword").asString();
                }else{
                    cname = record.get("aword").asString()+" "+record.get("bword").asString();
                }
                String coalesce_query = "MATCH (a:PERSON"+dId_sId+")-[r:dep]->(b:PERSON"+dId_sId+") " +
                        "WHERE a.idx="+record.get("aidx").asInt()+" AND b.idx="+record.get("bidx").asInt()+" "+
                        "WITH a,b,r "+
                        "SET a.word =\""+cname+"\" "+
                        "DELETE r,b";

                session.run(coalesce_query);

                System.out.println("coalesced "+record.get("aword" ).asString()+" AND " +record.get("bword").asString());


            }
        }
    }

    public void find_NER_and_unwanted(String docId,int sen_id){

        StatementResult result;
        String dId_sId="{docId:"+docId+" , senId:"+sen_id+"}";
        try (Session session = this.neo4jDriver.session()) {
            String get_persons_query = "MATCH (a:PERSON"+dId_sId+") RETURN a.word as aword,a.idx as aidx";
            result=session.run(get_persons_query);

            while (result.hasNext()){

                Record record = result.next();
                String pname = record.get("aword").asString();
                known_entity_search_result ke = is_this_named_entity_needed(pname);
                if (!ke.is_needed){
                    System.out.println("find_NER_and_unwanted() : unknown person : "+pname);
                    /*String discard_person_query = "MATCH (a:PERSON"+dId_sId+")-[r:dep]-() "+
                            "WHERE a.idx="+record.get("aidx").asInt()+" "+
                            "WITH a,r "+
                            "DELETE a,r ";

                    session.run(discard_person_query);*/


                }
                else {

                    String rename_person_to_dictionary_name_query=
                            "MATCH (a:PERSON"+dId_sId+") " +
                            " WHERE a.idx="+record.get("aidx").asInt()+" " +
                            " SET a.word = \""+this.sen_names.get(ke.found_at)+"\" "+
                            " SET a.k_id = "+ke.found_at+" " +
                            " SET a :D_PERSON";
                    session.run(rename_person_to_dictionary_name_query );

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
        for (Object al_sen:this.sen_alias.keySet()){
            String al_sen_s = String.valueOf(al_sen);
            if (al_sen_s.equalsIgnoreCase(nst)){
                System.out.println("known_entity_search_result: found: "+al_sen);
                ret=new known_entity_search_result(true,Integer.parseInt(sen_alias.get(al_sen).toString()));
                return ret;
            }
            else{
                System.out.println("known_entity_search_result: not found: "+al_sen);
            }

        }


        return (new known_entity_search_result(false,0));
    }

    public  String array_to_list_query(ArrayList <String> sl){

        String ret = "[";
        for(String est:sl){
            ret=ret+"\""+est+"\",";

        }
        if (ret.length()>1){
            ret = ret.substring(0,ret.length()-1);
        }
        ret = ret+"]";
        return ret;
    }



    /**
     * Filter out unexpected characters from relation ship names
     *
     * current rules :
     *     replace  - with _
     *
     */

    public String ret_rel_type(String ret){

        ret = ret.replaceAll(Pattern.quote("-"),"_");
        System.out.println("ret_rel_type() : "+ret);
        return ret;

    }



    public String get_rel_type(ArrayList<String> w_l, ArrayList<String> t_l){
        String ret ;
        if (t_l.contains("VB")){
            ret = w_l.get(t_l.indexOf("VB"));
            return ret_rel_type(ret);
        }else if(t_l.contains("VBN")){
            ret = w_l.get(t_l.indexOf("VBN"));
            return ret_rel_type(ret);
        }else if(t_l.contains("VBD")){
            ret = w_l.get(t_l.indexOf("VBD"));
            return ret_rel_type(ret);
        }else if(t_l.contains("VBZ")){
            ret = w_l.get(t_l.indexOf("VBZ"));
            return ret_rel_type(ret);
        }
        return ret_rel_type("CO_REFERRED");
    }


    public void traverse_path_connecting_ners(String docId,int sen_id){
        String dId_sId="{docId:"+docId+" , senId:"+sen_id+"}";
        StatementResult res,p_result,e_res;
        ArrayList<String> w_list = new ArrayList<String>();
        ArrayList<String> t_list = new ArrayList<String>();
        String rel_type;


        try (Session session = this.neo4jDriver.session()) {


            //Query to find shortest paths between persons that have k_id
            p_result = session.run("MATCH (n:PERSON"+dId_sId+") " +
                    "WHERE EXISTS(n.k_id)" +
                    "WITH collect(n) as nodes " +
                    "UNWIND nodes as n " +
                    "UNWIND nodes as m " +
                    //"WITH * WHERE (id(n) < id(m)) AND (n.k_id <> m.k_id) " +
                    "WITH * WHERE (id(n) < id(m)) " +
                    "MATCH path = allShortestPaths( (n)-[*..15]-(m) ) " +
                    "RETURN path");
            while (p_result.hasNext()){
                Record record  = p_result.next();
                //System.out.println(record.keys());
                //System.out.println(record.values().getClass().getName());
                //System.out.println("###############");
                Path pt = (record.values()).get(0).asPath();


                //get the start node and end node ; check if the main relation already exists
                String rel_exist_query =
                        "MATCH (n:PERSON{k_id:"+pt.start().get("k_id").asInt()+",docId:"+docId+",idx:"+pt.start().get("idx").asInt()+"}), " +
                        "(m:PERSON{k_id:"+pt.end().get("k_id").asInt()+",docId:"+docId+",idx:"+pt.end().get("idx").asInt()+"}) " +
                        "OPTIONAL MATCH (n)-[r]->(m) " +
                        "OPTIONAL MATCH (m)-[r1]->(n) " +
                        "WHERE ( r.type=\"MAIN\") OR ( r1.type=\"MAIN\")\n" +
                        "RETURN r,r1";
                res  = session.run(rel_exist_query);
                //System.out.println("##rel_exists start##");
                Record rel_exist_rd = res.next();
                if (rel_exist_rd.toString().contains("{r: NULL, r1: NULL}")){
                    if (pt.start().get("k_id").asInt()==pt.end().get("k_id").asInt()) continue;
                    // there are NULL results for rel_exist_query. So path of type MAIN doesn't exists
                    System.out.println("rel doesn't exist for "+pt.start().get("word")+"to "+pt.end().get("word") );
                }
                else {
                    System.out.println("rel exists for "+pt.start().get("word")+"to "+pt.end().get("word") );
                    continue;
                }
                //System.out.println(res.next().keys());
                //System.out.println(res.next().values());
                //System.out.println("##rel_exists end##");




                Iterable<Node> nds = pt.nodes();
                for (Node nd : nds ){

                    if (nd==pt.start() ) continue;
                    if (nd==pt.end()){
                        rel_type = get_rel_type(w_list,t_list);
                        System.out.println("Creating MAIN relation : "+pt.start().get("word")+" --> "+nd.get("word"));
                        String query = "MATCH (n:PERSON"+dId_sId+") , (m:PERSON"+dId_sId+") " +
                                "WHERE n.k_id ="+pt.start().get("k_id").asInt() +"" +
                                        " AND m.k_id ="+pt.end().get("k_id").asInt()+" " +
                                        " AND id(n) ="+pt.start().id()+ " "+
                                        " AND id(m) ="+pt.end().id()+ " "+
                                "CREATE (n)-[r:"+rel_type+"]->(m) " +
                                "SET r.w_list= " + array_to_list_query(w_list)+" "+
                                "SET r.t_list= " + array_to_list_query(t_list)+" "+
                                "SET r.type =  \"MAIN\" "+
                                "RETURN r" ;

                        System.out.println(query);
                        e_res = session.run(query);
                        w_list.clear();
                        t_list.clear();
                        continue;
                    }

                    if (nd.hasLabel("PERSON")){
                        //This is a complex path connecting more than two PERSON , discard this path
                        System.out.println("Exiting complex path: "+pt.start().get("word") +" -- "+ nd.get("word").asString()+" -> "+pt.start().get("word") );
                        w_list.clear();
                        w_list.clear();
                        break;
                    }

                    w_list.add(nd.get("word").asString());
                    t_list.add(nd.get("tag") .asString());



                }


            }

        }


    }
}

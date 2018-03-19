package AnalyticsProcessor;

import java.util.HashMap;
import java.util.List;


public class QueryBuilder {

    HashMap  queries;

    //Query to coalesce words connected with "compound" edge to Person:
    //  *These words (next to person) will be collected and assigned to corresponding person's alias attribute
    String ccntp = "MATCH (n:PERSON %s)-[r]-(m) WHERE r.type=\"compound\" AND size((m)--())=1 \n" +
            "WITH n,m \n" +
            "ORDER BY m.idx \n" +
            "WITH n,collect(m) as c_list  \n" +
            "FOREACH (cmp in c_list| SET n.alias= n.alias+cmp.word+\" \" DETACH DELETE cmp ) RETURN n";

    String sap = " MATCH (n:PERSON %s) SET n.alias=\"\"";

    String ccw = "MATCH (n %s)-[r]-(m) WHERE r.type=\"compound\" AND size((m)--())=1 AND m.idx<n.idx \n" +
            "WITH n,m  ORDER BY m.idx DESC \n" +
            "WITH n,collect(m) as c_list  \n" +
            "SET n:COMPUONDED\n" +
            "FOREACH (cmp in c_list| SET n.word = cmp.word+\" \"+n.word DETACH DELETE cmp ) \n" +
            "RETURN n\n" +
            "UNION ALL\n" +
            "MATCH (n %s)-[r]-(m) WHERE r.type=\"compound\" AND size((m)--())=1 AND m.idx>n.idx \n" +
            "WITH n,m  ORDER BY m.idx  \n" +
            "WITH n,collect(m) as c_list  \n" +
            "SET n:COMPUONDED\n" +
            "FOREACH (cmp in c_list| SET n.word = n.word+\" \"+cmp.word DETACH DELETE cmp ) \n" +
            "RETURN n";




    public QueryBuilder() {

        this.queries = new HashMap<String,String>();

        queries.put("coalesce_compounds_next_to_persons",ccntp);
        queries.put("set_alias_persons",sap);
        queries.put("coalesce_compound_words",ccw);

    }


    //HashMap<String, String> queries = new HashMap<String,String>();

    //queries.put("adsf","MATCH");

    public String buildquery(String qr, List<Object> args){

        String ret,q_str;

        q_str = this.queries.get(qr).toString();
        ret = String.format(q_str,args.toArray());

        return ret;
    }

}

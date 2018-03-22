package AnalyticsProcessor;

import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


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

    //Query to reduce appos edges next to person nodes - step1
    String  raenp_1 ="MATCH (n:PERSON %s)-[r]-(m) WHERE r.type=\"appos\"\n" +
            "WITH n,m\n" +
            "MATCH (m)-[r1]-(a) WHERE r1.type<>\"appos\"\n" +
            "WITH n,collect(r1) as rels ,collect(a) as others\n" +
            "WITH n, rels,others,range(0,size(rels)-1) as idx\n" +
            "UNWIND idx as i\n" +
            "\tWITH others[i] as ot ,rels[i] as re,n\n" +
            "    CREATE (n)-[r:dep]->(ot)\n" +
            "    SET r=re\n" +
            "    DELETE re\n" +
            "RETURN *";
    String  raenp_2 =
            "MATCH (n:PERSON %s)-[r]-(m) WHERE r.type=\"appos\" AND size((m)--())=1 \n" +
            "SET n += {title:m.word}\n" +
            "DETACH DELETE m\n" +
            "RETURN n";


    //Query to check if there are two D_persons connected with "conj_and". Returns TRUE if there are such nodes in sentence
    String if_2_dp_ca =
                    "MATCH (n:D_PERSON %s)-[r]-(m:D_PERSON) \n" +
                    "WHERE r.type=\"conj_and\"\n" +
                    "RETURN count(n) > 0 as result";

    //Query to check if there is an "conj_and" connecting a D_PERSON and a D_PERSON_GROUP
    String if_dp_ca_pg =
                    "MATCH (n:D_PERSON_GROUP %s )-[r]-(m:D_PERSON) \n" +
                    "WHERE r.type=\"conj_and\"\n" +
                    "RETURN count(n) > 0 as result";
    //Query to check if there is an "conj_and" connecting a D_PERSON and a D_PERSON_GROUP
    String if_pg_ca_pg =
            "MATCH (n:D_PERSON_GROUP %s )-[r]-(m:D_PERSON_GROUP) \n" +
                    "WHERE r.type=\"conj_and\"\n" +
                    "RETURN count(n) > 0 as result";


    //Query to reduce two D_Persons connected by "conj_and" to a person group.
    String r_dp_ca_pg =
                    "MATCH (n:D_PERSON %s)-[r_and]-(m:D_PERSON) \n" +
                    "WHERE r_and.type=\"conj_and\"\n" +
                    "WITH n,m,r_and  ORDER BY n.k_id  LIMIT 1\n" +
                    "  OPTIONAL MATCH (n)-[r]-(others_n) WHERE id(others_n)<>id(m)\n" +
                    "  WITH n,m,r_and,collect(r) as rln ,collect(others_n) as otn\n" +
                    "    OPTIONAL MATCH (m)-[r]-(others_m) WHERE id(others_m)<>id(n)\n" +
                    "    WITH n,m,r_and,rln,otn,collect (r) as rlm,collect(others_m) as otm\n" +
                    "      WITH n,m,r_and,rln,otn,rlm,otm,range(0,size(rln)-1) as idx_n,range(0,size(rlm)-1) as idx_m\n" +
                    "      CREATE (n)-[r:part_of]->(pg:D_PERSON_GROUP)<-[r1:part_of]-(m)\n" +
                    "      SET pg+=n\n" +
                    "      SET pg.word=n.word+\",\"+m.word\n" +
                    "      SET pg.k_id=[n.k_id]+[m.k_id]\n" +
                    "      DELETE r_and\n" +
                    "      WITH pg,rln,otn,rlm,otm,idx_n,idx_m\n" +
                    "        UNWIND idx_n as i_n \n" +
                    "        WITH pg,otn[i_n] as ot_n , rln[i_n] as rl_n ,rlm,otm,idx_m\n" +
                    "          UNWIND idx_m as i_m \n" +
                    "          WITH pg,ot_n,rl_n,otm[i_m] as ot_m ,rlm[i_m] as rl_m\n" +
                    "            MERGE (ot_n)-[r:dep]-(pg)\n" +
                    "            SET r = rl_n\n" +
                    "            DELETE rl_n\n" +
                    "            MERGE (ot_m)-[r1:dep]-(pg)\n" +
                    "            SET r1= rl_m\n" +
                    "            DELETE rl_m\n" +
                    "            RETURN pg,ot_n,rl_n,ot_m,rl_m";

    //Query to reduce conj_and edges between D_Person_group and D_PERSON
    String r_ca_pg_dp =
                    "MATCH (n:D_PERSON_GROUP %s)-[r_and]-(m:D_PERSON) \n" +
                    "WHERE r_and.type=\"conj_and\"\n" +
                    "WITH n,m,r_and LIMIT 1\n" +
                    "  OPTIONAL MATCH (m)-[r]-(others_m) WHERE id(others_m)<>id(n)\n" +
                    "  WITH n,m,r_and,collect (r) as rlm,collect(others_m) as otm\n" +
                    "    WITH n,m,r_and,rlm,otm,range(0,size(rlm)-1) as idx\n" +
                    "    SET n.word = n.word+\",\"+m.word\n" +
                    "    SET n.k_id = n.k_id + [m.k_id]\n" +
                    "    CREATE (n)<-[r:part_of]-(m)\n" +
                    "    DELETE r_and\n" +
                    "    WITH n,rlm,otm,idx\n" +
                    "      UNWIND idx as i\n" +
                    "      WITH n,otm[i] as ot ,rlm[i] as rl \n" +
                    "        MERGE (n)-[r:dep]-(ot)\n" +
                    "        SET r=rl \n" +
                    "        DELETE rl \n" +
                    "        RETURN n,ot,rl ";

    //Query to reduce conj_and edges between two d_person_group
    String r_pg_ca_pg =
                    "MATCH (n:D_PERSON_GROUP %s )-[r_and]-(m:D_PERSON_GROUP) \n" +
                    "WHERE r_and.type=\"conj_and\"\n" +
                    "WITH n,m,r_and LIMIT 1\n" +
                    "  OPTIONAL MATCH (m)-[r]-(others_m) WHERE id(others_m)<>id(n)\n" +
                    "  WITH n,m,r_and,collect (r) as rlm,collect(others_m) as otm\n" +
                    "    WITH n,m,r_and,rlm,otm,range(0,size(rlm)-1) as idx\n" +
                    "    SET n.word = n.word+m.word\n" +
                    "    SET n.k_id = n.k_id + m.k_id\n" +
                    "    DELETE r_and,m \n" +
                    "    WITH n,rlm,otm,idx\n" +
                    "      UNWIND idx as i\n" +
                    "      WITH n,otm[i] as ot ,rlm[i] as rl \n" +
                    "        MERGE (n)-[r:dep]-(ot)\n" +
                    "        SET r=rl \n" +
                    "        DELETE rl \n" +
                    "        RETURN n,ot,rl";


    //Query to remove temporal data to d_person nodes
    String rem_t_d_p =
                    "MATCH (n:D_PERSON)\n" +
                    "WHERE EXISTS(n.docId)\n" +
                    "REMOVE n.Date,n.Time,n.docId,n.senId,n.source,n.idx,n.tag\n" +
                    "RETURN n";

    //Query to check if multiple d_persons with same k_id exists
    String if_s_d_p_e =
                    "MATCH (n:D_PERSON),(m:D_PERSON)\n" +
                    "WHERE n.k_id=m.k_id AND id(n)<id(m)\n" +
                    "RETURN count(n) > 0 as result";

    //Query to get the multiple d_persons names and dup_counts
    String get_m_d_p =
                    "MATCH (n:D_PERSON),(m:D_PERSON) \n " +
                    "WHERE n.k_id=m.k_id AND id(n)<id(m) \n" +
                    "WITH n.word as name, count(n) as dup_count \n" +
                    "RETURN name,dup_count";

    //Query to merge d_persons with same k_id
    String m_d_p_same_k_id=
                    "MATCH (n:D_PERSON),(m:D_PERSON) \n" +
                    "WHERE n.k_id=m.k_id AND id(n)<id(m)\n" +
                    "WITH n,m LIMIT 1 \n" +
                    "  MATCH (m)-[r]-(other_m) WHERE id(other_m)<>id(n)\n" +
                    "  WITH n,m,collect(r) as rels_m ,collect(other_m) as ot_m\n" +
                    "    WITH n,m,rels_m,ot_m,range(0,size(rels_m)-1) as idx \n" +
                    "      SET n.alias = n.alias+m.alias\n" +
                    "      SET n.title = n.title+n.title\n" +
                    "      DELETE m \n" +
                    "      WITH n,rels_m,ot_m,idx \n" +
                    "        UNWIND idx as i \n" +
                    "        WITH n,ot_m[i] as ot,rels_m[i] as rel\n" +
                    "          WITH n,ot,rel,type(rel) as tr\n" +
                    "            MERGE (n)-[r:tr]-(ot)\n" +
                    "            SET r=rel \n" +
                    "            DELETE rel \n" +
                    "            RETURN n,ot,rel";

    //Query to get relationships and nodes connected to the second node with same k_id
    String get_r_n_k_id  =
                    "MATCH (n:D_PERSON),(m:D_PERSON) \n" +
                    "WHERE n.k_id=m.k_id AND id(n)<id(m)\n" +
                    "WITH n,m LIMIT 1 \n" +
                    "  MATCH (m)-[r]-(other_m) WHERE id(other_m)<>id(n)\n" +
                    "  WITH n,m,collect(r) as rels_m ,collect(other_m) as ot_m\n" +
                    "    WITH n,m,rels_m,ot_m,range(0,size(rels_m)-1) as idx \n" +
                    "      SET n.alias = n.alias+m.alias\n" +
                    "      SET n.title = n.title+n.title\n" +
                    "      WITH n,m,rels_m,ot_m,idx \n" +
                    "        UNWIND idx as i \n" +
                    "        WITH n,m,ot_m[i] as ot,rels_m[i] as rel\n" +
                    "          RETURN id(n) as id_n,id(m) as id_m ,id(ot) as id_ot ,rel,type(rel) as t_rel,id(rel) as id_rel";

    //Query to create a relationship for same_k_id
    String cr_rel_s_k_id =
                    "MATCH (n:D_PERSON),(ot) \n" +
                    "WHERE id(n)=%d AND id(ot)=%d \n" +
                    "MERGE (n)-[r:%s ]-(ot)";

    //Query to delete this relationship for same_k_id
    String del_rel_s_k_id =
            "MATCH ()-[r]-() WHERE id(r)=%d DELETE r";

    //Query to delete this node for same_k_id
    String del_node_s_k_id =
                    "MATCH (n)  " +
                    "WHERE id(n)=%d " +
                    "DELETE n";

    //Query to get all relations inferred from a sentence containing a word
    String get_inf_rel_f_w =
                    "MATCH (n)\n" +
                    "WHERE n.word CONTAINS \"%s\"\n" +
                    "WITH collect(n.docId) as docs\n" +
                    "UNWIND docs as e_doc\n" +
                    "MATCH ()-[r]->() WHERE toInteger(r.docId)=toInteger(e_doc)  \n" +
                    "RETURN startNode(r).word as n , r.docId as doc, type(r) as relation, r.w_list as words, endNode(r).word as m";




    public QueryBuilder() {

        this.queries = new HashMap<String,String>();

        queries.put("coalesce_compounds_next_to_persons",ccntp);
        queries.put("set_alias_persons",sap);
        queries.put("coalesce_compound_words",ccw);
        queries.put("reduce_appos_edges_1",raenp_1);
        queries.put("reduce_appos_edges_2",raenp_2);
        queries.put("check_if_2_d_persons_conj_and",if_2_dp_ca);
        queries.put("reduce_2_d_persons_conj_and_to_group",r_dp_ca_pg);
        queries.put("check_if_d_person_conj_and_person_group",if_dp_ca_pg);
        queries.put("reduce_d_person_group_conj_and_d_person",r_ca_pg_dp);
        queries.put("check_if_person_group_conj_and_person_group",if_pg_ca_pg);
        queries.put("reduce_d_person_group_conj_and_d_person_group",r_pg_ca_pg);
        queries.put("remove_temporal_attributes_from_d_person",rem_t_d_p);
        queries.put("check_if_multi_d_persons_same_k_id",if_s_d_p_e);
        queries.put("merge_d_persons_same_k_id",m_d_p_same_k_id);    //This query doesn't work, as relationships cannot be passed in as parameters
        queries.put("get_rels_nodes_for_sec_k_id",get_r_n_k_id);
        queries.put("create_rels_nodes_same_k_id",cr_rel_s_k_id);
        queries.put("delete_this_relationship_with_id",del_rel_s_k_id);
        queries.put("delete_this_node_with_id",del_node_s_k_id);
        queries.put("get_multi_d_p_dup_count",get_m_d_p);
        queries.put("get_inferred_relations_for_news_containing_word",get_inf_rel_f_w);

    }


    public String buildquery_dynamic(String qr, List<Object> args, Map<String,Object> map){

        String q_str,ret;

        q_str = this.queries.get(qr).toString();
        ret = String.format(q_str,args.toArray());

        for(String key:map.keySet()){
            //System.out.println(map.get(key));
            if (key.equals("docId") || (key.equals("type")) || (key.equals("Date")) || (key.equals("Time")) || (key.equals("source")) ) {
                ret = ret + "\n SET r."+key+"=";
                ret = ret+"\""+map.get(key)+"\"";
            }else if (key.equals("senId")){
                ret = ret + "\n SET r."+key+"=";
                ret = ret+map.get(key);
            }else if ((key.equals("t_list")) ||(key.equals("w_list"))) {
                ret = ret + "\n SET r."+key+"=";
                ret = ret +map.get(key).toString().replace("[","[\"").replace(",","\",\"").replace("]","\"]");
            }
        }




        return ret;

    }

    public String result_as_tab_string(StatementResult res){
        Record rec;
        String ret="\n"+String.format("%-30s"," ") ;
        //ret += res.keys();
        for (String key :res.keys()){
            ret +=String.format("%-20s",key);
        }
        ret += "\n"+String.format("%-30s"," ");
        while(res.hasNext()){
            rec = res.next();
            for(String key: res.keys()){
                ret += String.format("%-20s",rec.get(key).toString());
            }
            ret += "\n"+String.format("%-30s"," ");

        }

        return ret;

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

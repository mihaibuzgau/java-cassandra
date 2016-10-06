package org.cloudfoundry.samples;

import java.lang.StringBuilder;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.io.IOException;
import java.io.PrintWriter;

import javax.annotation.PostConstruct;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.utils.UUIDs;

public class CassServlet  extends HttpServlet {

        private static final long serialVersionUID = 1L;
        private static final String KEYSPACE = "test";
        private static final String TABLE = "test";
        private Cluster cluster;
        private Session session;
        
        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
                response.setContentType("text/plain");
                response.setStatus(200);
                PrintWriter writer = response.getWriter();
               
                String cassHost = "";
				try {
					cassHost = getHost();
				} catch (Exception e) {
					writer.println(e.getMessage());
					e.printStackTrace();
				}
                
                cluster = Cluster.builder()
                        .addContactPoint(cassHost)
                        .build();
                session = cluster.connect();
                
                Select query = QueryBuilder.select()
                        .all()
                        .from(KEYSPACE, TABLE);
                List<Row> results = session.execute(query).all();
                for (Row row : results) {
                    writer.println(row.getString("time"));
                }

                
                writer.close();
        }
        
        protected void doPost(HttpServletRequest request, HttpServletResponse response)  throws ServletException, IOException {
            response.setContentType("text/plain");
            response.setStatus(201);
            PrintWriter writer = response.getWriter();
            
            String cassHost = "";
			try {
				cassHost = getHost();
			} catch (Exception e) {
				writer.println(e.getMessage());
				e.printStackTrace();
				return;
			}
            
            cluster = Cluster.builder()
                    .addContactPoint(cassHost)
                    .build();
            Metadata metadata = cluster.getMetadata();
            writer.println("Connected to cluster " + metadata.getClusterName());
            for (Host host : metadata.getAllHosts()) {
            	writer.println("Data center: " + host.getDatacenter() + "; IP address: " + host.getAddress());
            }
            session = cluster.connect();
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE
                    + " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':1};");
            session.execute("CREATE TABLE IF NOT EXISTS " + KEYSPACE +"." + TABLE +" ("
                    + "id uuid PRIMARY KEY, "
                    + "time text);");
            
            String timeStamp = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(Calendar.getInstance().getTime());
            writer.println("Request at:" + timeStamp);
            
            Insert insert = QueryBuilder.insertInto(KEYSPACE, TABLE)
                    .value("id", UUIDs.random())
                    .value("time", timeStamp);
            session.execute(insert);

            
            writer.close();
        }
        
        //Get cassandra host from environment variables
        private String getHost() throws Exception {
            if (System.getenv().containsKey("VCAP_SERVICES")) {

            	String serv;
            	serv = System.getenv().get("VCAP_SERVICES");
            	JSONObject obj = new JSONObject(serv);
            	
            	JSONArray arr;
            	arr = obj.getJSONArray(obj.keys().next());
            	
            	String cred;
            	cred = arr.getJSONObject(0).getJSONObject("credentials").getString("PASSTHROUGH_DATA");
            	
            	JSONObject credObj = new JSONObject(cred);
            	String host;
            	host = credObj.getString("cassandra_host");
            	
            	return host;
            }
            
            throw new Exception("Application is not bound to a cassandra service");
        }
}

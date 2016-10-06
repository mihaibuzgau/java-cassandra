package org.cloudfoundry.samples;

import java.lang.StringBuilder;
import java.util.*;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONArray;
import org.json.JSONObject;

public class JavaEnvServlet extends HttpServlet {

        private static final long serialVersionUID = 1L;

        protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
                response.setContentType("text/plain");
                response.setStatus(200);
                PrintWriter writer = response.getWriter();

        		StringBuilder sb = new StringBuilder();
                        Map<String, String> env = System.getenv();
                        for (String key : env.keySet()) {

                                sb.append(key + ": " + env.get(key)  + "\n");
                        }
                        

                        writer.println(sb.toString());
                        writer.close();
        }
}

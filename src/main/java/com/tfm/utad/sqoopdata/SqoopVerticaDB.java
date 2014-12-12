/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.tfm.utad.sqoopdata;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HTTP;
import org.apache.sqoop.Sqoop;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqoopVerticaDB {

    private static final String CONNECTION_DATA_BASE = "--connect";
    private static final String TABLE_DATA_BASE = "--table";
    private static final String INPUT_DIRECTORY = "--export-dir";
    private static final String USERNAME_DATA_BASE = "--username";
    private static final String PASSWORD_DATA_BASE = "--password";
    private static final String NUM_MAPPERS = "-m";

    private static final Map<String, String> config;
    private static final String HDFS_LOCALHOST_LOCALDOMAIN = "hdfs://172.16.134.128/";
    private static final String FS_DEFAULT_FS = "fs.defaultFS";

    private final static Logger LOG = LoggerFactory.getLogger(SqoopVerticaDB.class);

    static {
        config = new HashMap<>();
        config.put(CONNECTION_DATA_BASE, "jdbc:vertica://172.16.134.131:5433/tfm_utad_vertica");
        config.put(TABLE_DATA_BASE, "s1.coordinates");
        config.put(INPUT_DIRECTORY, "/home/jab/camus/verticadb");
        config.put(USERNAME_DATA_BASE, "dbadmin");
        config.put(PASSWORD_DATA_BASE, "ariza84");
        config.put(NUM_MAPPERS, "4");
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration(true);
        conf.set(FS_DEFAULT_FS, HDFS_LOCALHOST_LOCALDOMAIN);
        FileSystem fs = FileSystem.get(conf);
        Path filesPath = new Path(config.get(INPUT_DIRECTORY) + "/*/part-r*");
        FileStatus[] files = fs.globStatus(filesPath);
        for (FileStatus fStatus : files) {
            LOG.info("Path name:" + fStatus.getPath());
            Long minID = getID();
            int output = sqoopHDFStoVerticaDB(fStatus.getPath(), conf);
            if (output == 0) {
                LOG.info("Removing directory in path:" + fStatus.getPath().getParent());
                fs.delete(fStatus.getPath().getParent(), true);
                Long maxID = getID();
                sendDataToCartoDB(minID, maxID);
            } else {
                LOG.error("Sqoop FAILED exec file:" + fStatus.getPath() + ".Please, contact the system administrator.");
            }
        }
    }

    private static void sendDataToCartoDB(Long minID, Long maxID) {
        Connection conn = connect("jdbc:vertica://172.16.134.131:5433/tfm_utad_vertica", "dbadmin", "ariza84");
        if (conn != null) {
            LOG.info("Connection OK...");
            findBetweenMinIDAndMaxID(conn, minID, maxID);
        }
    }

    private static void sendDataToCartoDB(List<CoordinateCartoDB> result) {

        String url = "http://jab-utad.cartodb.com/api/v2/sql?q=";
        String api = "&api_key=00ffbd3278c07285554983d9713bc08c18d461b3";
        HttpClient httpclient = new DefaultHttpClient();
        HttpResponse response;
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO tfm_utad_wearables (_offset, userstr, created_date, activity, latitude, longitude, userid) VALUES ");
            for (int i = 0; i < result.size(); i++) {
                CoordinateCartoDB coordinateCartoDB = result.get(i);
                String date = coordinateCartoDB.getCreated_date().replace(' ', 'T');
                String insert;
                if (i < result.size() - 1) {
                    insert = "(" + coordinateCartoDB.getOffset() + ",'" + coordinateCartoDB.getUserstr() + "','" + date + "','" + coordinateCartoDB.getActivity() + "'," + String.valueOf(coordinateCartoDB.getLatitude()) + "," + String.valueOf(coordinateCartoDB.getLongitude()) + "," + coordinateCartoDB.getUserid() + "),";
                } else {
                    insert = "(" + coordinateCartoDB.getOffset() + ",'" + coordinateCartoDB.getUserstr() + "','" + date + "','" + coordinateCartoDB.getActivity() + "'," + String.valueOf(coordinateCartoDB.getLatitude()) + "," + String.valueOf(coordinateCartoDB.getLongitude()) + "," + coordinateCartoDB.getUserid() + ")";
                }
                sb.append(insert);
            }
            String encode = URLEncoder.encode(sb.toString(), HTTP.UTF_8)
                    .replaceAll("\\+", "%20")
                    .replaceAll("\\%21", "!")
                    .replaceAll("\\%27", "'")
                    .replaceAll("\\%28", "(")
                    .replaceAll("\\%29", ")")
                    .replaceAll("\\%2C", ",")
                    .replaceAll("\\%3A", ":")
                    .replaceAll("\\%27", "'")
                    .replaceAll("\\%7E", "~");
            URI uri = new URI(url + encode + api);
            HttpUriRequest request = new HttpGet(uri);
            LOG.info("Request sending to CartoDB...:" + request.getURI().toString());
            response = httpclient.execute(request);
            LOG.info("Response code:" + response.getStatusLine());
        } catch (UnsupportedEncodingException ex) {
            LOG.error("UnsupportedEncodingException:" + ex.toString());
        } catch (IOException ex) {
            LOG.error("IOException:" + ex.toString());
        } catch (URISyntaxException ex) {
            LOG.error("URISyntaxException:" + ex.toString());
        }
    }

    private static Long getID() {
        Long result = (long) 0;
        Connection conn = connect("jdbc:vertica://172.16.134.131:5433/tfm_utad_vertica", "dbadmin", "ariza84");
        if (conn != null) {
            result = findMaxID(conn);
        }
        return result;
    }

    private static Connection connect(String db_connect_str, String db_userid, String db_password) {
        Connection conn;
        try {
            Class.forName("com.vertica.jdbc.Driver").newInstance();
            conn = DriverManager.getConnection(db_connect_str, db_userid, db_password);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException e) {
            LOG.error("Connection error: " + e.toString());
            conn = null;
        }
        return conn;
    }

    private static void findBetweenMinIDAndMaxID(Connection conn, Long minID, Long maxID) {
        Statement stmt = null;
        String query;
        try {
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            query = "SELECT * FROM s1.coordinates WHERE id > " + minID + " AND id <= " + maxID + "";
            LOG.info("Query execution: " + query);
            ResultSet rs = stmt.executeQuery(query);
            int batch = 0;
            List<CoordinateCartoDB> result = new ArrayList<>();
            long start_time = System.currentTimeMillis();
            while (rs.next()) {
                batch++;
                CoordinateCartoDB cdb = new CoordinateCartoDB(
                        (long) rs.getInt("id"),
                        rs.getString("userstr"),
                        rs.getString("created_date"),
                        rs.getString("activity"),
                        rs.getFloat("latitude"),
                        rs.getFloat("longitude"),
                        (long) rs.getInt("userid"));
                result.add(cdb);
                if (batch == 50) {
                    sendDataToCartoDB(result);
                    batch = 0;
                    result = new ArrayList<>();
                }
            }
            if (batch > 0) {
                sendDataToCartoDB(result);
            }
            long end_time = System.currentTimeMillis();
            long difference = end_time - start_time;
            LOG.info("CartoDB API execution time: "
                    + String.format("%d min %d sec",
                            TimeUnit.MILLISECONDS.toMinutes(difference),
                            TimeUnit.MILLISECONDS.toSeconds(difference) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(difference))
                    ));
        } catch (SQLException e) {
            LOG.error("SQLException error: " + e.toString());
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    LOG.error("Statement error: " + ex.toString());
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    LOG.error("Connection error: " + ex.toString());
                }
            }
        }
    }

    private static Long findMaxID(Connection conn) {
        Long id = (long) 0;
        Statement stmt = null;
        String query;
        try {
            stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            query = "SELECT MAX(id) AS id FROM s1.coordinates";
            LOG.info("Query execution: " + query);
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                id = (long) rs.getInt("id");
            }
        } catch (SQLException e) {
            LOG.error("SQLException error: " + e.toString());
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                    LOG.error("Statement error: " + ex.toString());
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    LOG.error("Connection error: " + ex.toString());
                }
            }
        }
        return id;
    }

    private static int sqoopHDFStoVerticaDB(Path input, Configuration conf) {
        String arguments[] = new String[]{
            "export",
            "-Dsqoop.export.records.per.statement=1000",
            "--driver",
            "com.vertica.jdbc.Driver",
            "--connect",
            config.get(CONNECTION_DATA_BASE),
            "--table",
            config.get(TABLE_DATA_BASE),
            "--export-dir",
            input.toString(),
            "--username",
            config.get(USERNAME_DATA_BASE),
            "--password",
            config.get(PASSWORD_DATA_BASE),
            "--columns",
            "id,userstr,created_date,activity,latitude,longitude,userid",
            "-m",
            config.get(NUM_MAPPERS),
            "--input-fields-terminated-by",
            "\t",
            "--input-lines-terminated-by",
            "\n",
            "--connection-param-file",
            "/home/jab/sqoop.properties",
            "--batch",
            "--verbose"
        };
        return Sqoop.runTool(arguments, conf);
    }

    static class CoordinateCartoDB {

        private Long _offset;
        private String userstr;
        private String created_date;
        private String activity;
        private Float latitude;
        private Float longitude;
        private Long userid;

        public CoordinateCartoDB() {

        }

        public CoordinateCartoDB(Long _offset, String userstr, String created_date, String activity, Float latitude, Float longitude, Long userid) {
            this._offset = _offset;
            this.userstr = userstr;
            this.created_date = created_date;
            this.activity = activity;
            this.latitude = latitude;
            this.longitude = longitude;
            this.userid = userid;
        }
        
        public Long getOffset() {
            return _offset;
        }
        
        public void setOffset(Long _offset) {
            this._offset = _offset;
        }
        
        public String getUserstr() {
            return userstr;
        }
        
        public void setUserstr(String userstr) {
            this.userstr = userstr;
        }
        
        public String getCreated_date() {
            return created_date;
        }
        
        public void setCreated_date(String created_date) {
            this.created_date = created_date;
        }
        
        public String getActivity() {
            return activity;
        }
        
        public void setActivity(String activity) {
            this.activity = activity;
        }
        
        public Float getLatitude() {
            return latitude;
        }
        
        public void setLatitude(Float latitude) {
            this.latitude = latitude;
        }
        
        public Float getLongitude() {
            return longitude;
        }
        
        public void setLongitude(Float longitude) {
            this.longitude = longitude;
        }
        
        public Long getUserid() {
            return userid;
        }
        
        public void setUserid(Long userid) {
            this.userid = userid;
        }
    }
}
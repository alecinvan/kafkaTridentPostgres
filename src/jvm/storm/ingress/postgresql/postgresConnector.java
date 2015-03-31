package storm.ingress.postgresql;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Date;
import java.util.LinkedList;
import java.util.Properties;

/**
 * Created by sa on 9/18/2014.
 */

@SuppressWarnings("unused")
public class postgresConnector  {

        private class Events {
            public int id;
            public String event_object;
        }

        public void connectIngress() {
            Connection conn = null;
            conn = connectToDatabaseOrDie();
        }

        private Connection connectToDatabaseOrDie() {
            Connection conn = null;
            try {
                Class.forName("org.postgresql.Driver");
                String url = "jdbc:postgresql://10.100.70.23:5432/ingest";
                System.out.println("DB to be connected .....");
//                conn = DriverManager.getConnection(url, "sali", "StgDo92");
                conn = DriverManager.getConnection(url, "sali", "sali");
                System.out.println("DB connected .....");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                System.exit(1);
            } catch (SQLException e)

            {
                e.printStackTrace();
                System.exit(2);
            }
            return conn;
        }
}

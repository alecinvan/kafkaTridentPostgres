package storm.ingress.postgresql;

/**
 * Created by sa on 9/18/2014.
 */
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import org.postgresql.copy.*;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
//import org.postgresql.Driver;

import org.apache.log4j.Logger;

import storm.trident.state.OpaqueValue;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.CachedMap;
import storm.trident.state.map.IBackingMap;
import storm.trident.state.map.NonTransactionalMap;
import storm.trident.state.map.OpaqueMap;
import storm.trident.state.map.TransactionalMap;
import backtype.storm.task.IMetricsContext;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class PostgresqlState<T> implements IBackingMap<T> {

    private Connection conn = null;
    private PostgresqlStateConfig config;
    private static final Logger logger = Logger.getLogger(PostgresqlState.class);

    PostgresqlState(final PostgresqlStateConfig config) {
        this.config = config;
        try {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(config.getUrl(), "sali", "sali");
            System.out.println("ingest DB connectted .......");
            CopyManager cm = new CopyManager((BaseConnection) conn);        // define copy manager
        } catch ( ClassNotFoundException e) {
            logger.error("Failed to establish DB connection - 1", e);
            System.exit(1);
        } catch (SQLException e) {
            logger.error("Failed to establish DB connection - 2", e);
            System.exit(2);
        }

    }

    /**
     * factory method for the factory
     *
     * @param config
     * @return
     */
    public static Factory newFactory(final PostgresqlStateConfig config) {
        return new Factory(config);//NonTransactionalMap.build(new PostgresqlState(config));
    }


    /**
     * multiget implementation for postgresql
     *
     */
    @Override
    @SuppressWarnings({"unchecked","rawtypes"})
    public List<T> multiGet(final List<List<Object>> keys) {

        final List<T> result = new ArrayList();

        for (final List<Object> key : keys) {
                   result.add((T) null) ;
        }
        return result;
    }

    /**
     * multiput implementation for db
     *
     */
    @Override
    public void multiPut(final List<List<Object>> keys, final List<T> values) {

        System.out.println("multiPut start ..... ");
        System.out.println(keys);

                       // partitions the keys and the values and run it over every one
        final Iterator<List<List<Object>>> partitionedKeys = Lists.partition(keys, config.getBatchSize()).iterator();
        final Iterator<List<T>> partitionedValues = Lists.partition(values, config.getBatchSize()).iterator();
        System.out.println("+++ [[K]] " + keys.size() + "\n+++ [[V]]" + values.size() + "\n--- [[B]]" + config.getBatchSize());

/*
        while (partitionedKeys.hasNext() && partitionedValues.hasNext()) {
                final List<List<Object>> pkeys = partitionedKeys.next();
                final List<T> pvalues = partitionedValues.next();
                System.out.println("===K [[K]] " + pkeys + "\n===< [[V]]" + pvalues);

                final StringBuilder copyQueryBuilder = new StringBuilder()
                               .append("COPY ")
                               .append(config.getTable())
                               .append("(")
                               .append(buildColumns())
                               .append(") FROM STDIN WITH DELIMITER '|'");

                System.out.println("copyQueryBuilder: " + copyQueryBuilder);
                final List<String> combineRowList = new ArrayList() ;
                byte[] combineRowBytes ;
                for (int i = 0; i < pkeys.size(); i++)  {
                          for (int j = 0; j < pkeys.get(i).size() ; j++)
                          {
                                  String row = new String(pkeys.get(i).get(j) + "|" +  pvalues.get(i) + "\n") ;
                                  combineRowList.add(row);
                                  System.out.println("Row:" + i + ":" + j + ":" + row);
                          }
                }

                ByteArrayOutputStream baos = new ByteArrayOutputStream() ;
                DataOutputStream out = new DataOutputStream(baos) ;
                for (String element: combineRowList) {
                       try{
                              out.write(element.getBytes()) ;
                           }
                       catch (Exception ex)
                       {
                            logger.error("Problem in createByteArray", ex);
                       }
                }
                combineRowBytes = baos.toByteArray();
//                System.out.println(new String(combineRowBytes));

                CopyIn cpIN=null;
                try {
                      CopyManager cm = new CopyManager((BaseConnection) conn);
                      cpIN=cm.copyIn(copyQueryBuilder.toString());
                      cpIN.writeToCopy(combineRowBytes,0,combineRowBytes.length);
                      cpIN.endCopy();
                      System.out.println("Below Values are inserted");
                } catch (final SQLException ex) {
                      logger.error("Multiput update failed", ex);
                } finally {
                           try
                           {
                                if (conn!=null)
                                     conn.close();
                           }
                           catch (SQLException ex) {
                                logger.error("Multiput update failed", ex);
                           }
                }

//                logger.debug(String.format("%1$d keys flushed", pkeys.size()));
        }


*/
        while (partitionedKeys.hasNext() && partitionedValues.hasNext()) {

            final List<List<Object>> pkeys = partitionedKeys.next();
            final List<T> pvalues = partitionedValues.next();  
                  // how many params per row of data opaque => keys + 2 * vals + 1 ; transactional => keys + vals + 1 ;  non-transactional => keys + vals
            int paramCount = 0;
            switch (config.getType()) {
                case OPAQUE:
                    paramCount += config.getValueColumns().length;
                case TRANSACTIONAL:
                    paramCount += 1;
                default:
                    paramCount += (config.getKeyColumns().length + config.getValueColumns().length);
            }
            final StringBuilder queryBuilder = new StringBuilder()
                    .append("WITH ")
                    .append(" new_values (")
                    .append(buildColumns())
                    .append(") AS (")
                    .append("VALUES ")
                    .append(Joiner.on(", ").join(repeat("(" + Joiner.on(",").join(repeat("?", paramCount)) + ")", pkeys.size())))
                    .append(")")
                    .append("INSERT INTO ").append(config.getTable())
                    .append("(").append(buildColumns()).append(") ")
                    .append("SELECT ").append(buildColumns()).append(" ")
                    .append("FROM new_values ") ;

                        // run the update
            System.out.println("+query = " + queryBuilder.toString());
            final List<Object> params = flattenPutParams(pkeys, pvalues);

            PreparedStatement ps = null;
            int i = 0;
            try {
                ps = conn.prepareStatement(queryBuilder.toString());
                for (final Object param : params) {
                       ps.setObject(++i, param);
                }
                ps.execute();
            } catch (final SQLException ex) {
                logger.error("Multiput update failed", ex);
            } finally {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException ex) {
                        // don't care
                    }
                }
            }
            logger.debug(String.format("%1$d keys flushed", pkeys.size()));
        }

  }

    private byte [] createByteArray( Object obj)
    {
        byte [] bArray = null;
        try
        {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream objOstream = new ObjectOutputStream(baos);
                objOstream.writeObject(obj);
                bArray = baos.toByteArray();
        }
        catch (Exception ex)
        {
                logger.error("Problem in createByteArray", ex);

        }
        return bArray;
    }

    private String buildColumns() {
        final List<String> cols = Lists.newArrayList(config.getKeyColumns()); // the columns for the composite unique key
        cols.addAll(getValueColumns());
//        final List<String> cols = Lists.newArrayList(config.getValueColumns());
        return Joiner.on(",").join(cols);
    }

    private String buildKeyQuery(final int n) {
        final String single = "(" + Joiner.on(" AND ").join(Lists.transform(Arrays.asList(config.getKeyColumns()), new Function<String, String>() {
            @Override
            public String apply(final String field) {
                return field + " = ?";
            }
        })) + ")";
        return Joiner.on(" OR ").join(repeat(single, n));
    }

    private List<String> getValueColumns() {
        final List<String> cols = Lists.newArrayList(config.getValueColumns()); // the columns storing the values
        if (StateType.OPAQUE.equals(config.getType()) || StateType.TRANSACTIONAL.equals(config.getType())) {
            cols.add("txid");
        }
        if (StateType.OPAQUE.equals(config.getType())) {
            cols.addAll(Lists.transform(Arrays.asList(config.getValueColumns()), new Function<String, String>() {
                @Override
                public String apply(final String field) {
                    return "prev_" + field;
                }
            })); // the prev_* columns
        }
        return cols;
    }

    /**
     * run the multi get query, passing in the list of keys and returning key tuples mapped to value tuples
     *
     * @param sql
     * @param keys
     * @return
     */
    private Map<List<Object>, List<Object>> query(final String sql, final List<List<Object>> keys) {

        final Map<List<Object>, List<Object>> result = new HashMap ();
        PreparedStatement ps = null;
        int i = 0;
        try {
            ps = conn.prepareStatement(sql);
            for (final List<Object> key : keys) {
//                    System.out.println("key = " + key) ;
                for (final Object keyPart : key) {
//                    System.out.println("keyPart = " + keyPart) ;
                    ps.setObject(++i, keyPart);
                }
            }
            final ResultSet rs = ps.executeQuery();
            final Function<String, Object> rsReader = new Function<String, Object>() {
                @Override
                public Object apply(final String column) {
                    try {
                        return rs.getObject(column);
                    } catch (final SQLException sqlex) {
                        return null;
                    }
                }
            };
            final List<String> keyColumns = Arrays.asList(config.getKeyColumns());
            final List<String> valueColumns = getValueColumns();
            while (rs.next()) {
                result.put(Lists.transform(keyColumns, rsReader), Lists.transform(valueColumns, rsReader));
//                System.out.println("result = " + result);
            }
            rs.close();
        } catch (final SQLException ex) {
            logger.error("multiget query failed", ex);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ex) {
                    // don't care
                }
            }
        }
        return result;

    }

    @SuppressWarnings("rawtypes")
    private List<Object> flattenPutParams(final List<List<Object>> keys, final List<T> values) {
       System.out.println("+++ keys = " + keys + "\n\t[KeySize[[" + keys.size() + "]]]\n");
       System.out.println("--- values = " + values + "\n\t[ValueSize[[" + values.size() + "]]\n" );
       final List<Object> flattenedRows = new ArrayList();
        for (int i = 0; i < keys.size(); i++) {
            flattenedRows.addAll(keys.get(i));
//            System.out.println("flattenedRows = " + flattenedRows + "\n" );
            switch (config.getType()) {
                case OPAQUE:
                    flattenedRows.addAll(valueToParams(((OpaqueValue) values.get(i)).getCurr()));
                    flattenedRows.add(((OpaqueValue) values.get(i)).getCurrTxid());
                    flattenedRows.addAll(valueToParams(((OpaqueValue) values.get(i)).getPrev()));
                    break;
                case TRANSACTIONAL:
                    flattenedRows.addAll(valueToParams(((TransactionalValue) values.get(i)).getVal()));
                    flattenedRows.add(((TransactionalValue) values.get(i)).getTxid());
                    break;
                default:
                 //   System.out.println("valueToParams = " + valueToParams(values.get(i)) + "\n" );
                    flattenedRows.addAll(valueToParams(values.get(i)));
            }
        }
        System.out.println("### [" + flattenedRows + "###"); 
        return flattenedRows;
    }

    @SuppressWarnings("unchecked")
    private List<Object> valueToParams(final Object value) {
        if (!(value instanceof List)) {
            return repeat(value, config.getValueColumns().length);
        } else {
            return (List<Object>) value;
        }
    }

    private <U> List<U> repeat(final U val, final int count) {
        final List<U> list = new ArrayList();
        for (int i = 0; i < count; i++) {
            list.add(val);
        }
        return list;
    }

    @SuppressWarnings("serial")
    static class Factory implements StateFactory {
        private PostgresqlStateConfig config;

        Factory(final PostgresqlStateConfig config) {
            this.config = config;
        }


        @Override
        @SuppressWarnings({"rawtypes","unchecked"})
        public State makeState(final Map conf, final IMetricsContext context, final int partitionIndex, final int numPartitions) {
            final CachedMap map = new CachedMap(new PostgresqlState(config), config.getCacheSize());
            switch (config.getType()) {
                case OPAQUE:
                    return OpaqueMap.build(map);
                case TRANSACTIONAL:
                    return TransactionalMap.build(map);
                default:
                    return NonTransactionalMap.build(map);
            }
        }
    }
}

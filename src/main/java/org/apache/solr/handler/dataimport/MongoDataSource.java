package org.apache.solr.handler.dataimport;

import com.mongodb.*;
import com.mongodb.util.JSON;
import java.net.UnknownHostException;
import java.util.*;

/**
 *
 * @author Justina Chen <rcfans@163.com>
 */
public class MongoDataSource extends DataSource<Iterator<Map<String, Object>>> {

    private static final String DATABASE = "db";
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String COLLECTION = "collection";
    private String database;
    private String collection;
    private Mongo mongo;
    private List<Map<String, String>> fields;

    @Override
    public void init(Context context, Properties initProps) {
        String host = initProps.getProperty(HOST);
        String port = initProps.getProperty(PORT, "27017");
        database = initProps.getProperty(DATABASE);
        collection = context.getEntityAttribute(COLLECTION);
        this.fields = context.getAllEntityFields();

        if (database == null || collection == null) {
            throw new DataImportHandlerException(
                    DataImportHandlerException.SEVERE, "Database & Collection name are needed.");
        }

        try {
            mongo = new Mongo(host, Integer.parseInt(port));
        } catch (UnknownHostException e) {
            throw new DataImportHandlerException(
                    DataImportHandlerException.SEVERE, "Can't connect to MongoDB server.");
        }
    }

    @Override
    public Iterator<Map<String, Object>> getData(String query) {        
        DBObject queryObject = (DBObject) JSON.parse(query);
        DBCursor cusor = mongo.getDB(database).getCollection(collection).find(queryObject);
        CursorIterator result = new CursorIterator(cusor);
        return result.getIterator();
    }

    @Override
    public void close() {
        mongo.close();
    }

    private class CursorIterator {
        private Iterator<Map<String, Object>> rIterator;

        public CursorIterator(final DBCursor cursor) {
            rIterator = new Iterator<Map<String, Object>>() {
                public boolean hasNext() {
                    return cursor.hasNext();
                }

                public Map<String, Object> next() {
                    return nextObject(cursor);
                }

                public void remove() {
                }
            };
        }

        public Iterator<Map<String, Object>> getIterator() {
            return rIterator;
        }

        private Map<String, Object> nextObject(DBCursor cursor) {
            DBObject object = cursor.next();
            Map<String, Object> result = new HashMap<String, Object>();
            for (String key : object.keySet()) {
                for (Map<String, String> fieldMap : fields) {
                    if (fieldMap.containsKey(key)) {
                        result.put(fieldMap.get(key), object.get(key));
                    }
                }
            }
            return result;
        }
    }
}

package com.semantix;

import java.util.*;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomHostInterceptor implements Interceptor {

    private static final Logger LOG = LoggerFactory.getLogger(CustomHostInterceptor.class);

    public CustomHostInterceptor() {
    }

    public void initialize() {
    }

    public Event intercept(Event event) {
        byte[] json = event.getBody();
//        it seems that tha content comes as an JSON object not an array
//        JSONArray jsonArray = new JSONArray(new String(json));
//        JSONObject object = jsonArray.getJSONObject(0);
        try {
            LOG.info("JSON: " + event.toString());
            JSONObject object = new JSONObject(new String(json, "UTF-8"));
//            String flattenedJson = FlattenJSON(new String(eventBody));
            Map<String, String> out = new HashMap<String, String>();
            Set<String> keySet = out.keySet();
            parse(object, out);
//            System.out.println("KEYSET:" + keySet);
//            String jsonCsv = "";
            //order is a problem so we cannot just put the elements on the csv
//            for (String s : keySet) {
////                System.out.println("VALUE: " + s);
//                jsonCsv += out.get(s) + ",";
//            }
//            //get the index of the last comma to remove it
//            jsonCsv = jsonCsv.substring(0, jsonCsv.lastIndexOf(","));
            String alarm_name = out.get("alarm_name");
            String object1 = out.get("object");
            String status = out.get("status");
            String startts = out.get("startts");
            String endts = out.get("endts");
            String urgency = out.get("urgency");
            String jsonCsv = alarm_name + "," + object1 + "," + status + "," + startts + "," + endts + "," + urgency;
            event.setBody(jsonCsv.getBytes("UTF-8"));
            return event;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return event;
    }


    public List<Event> intercept(List<Event> events) {

        List<Event> interceptedEvents =
                new ArrayList<Event>(events.size());
        for (Event event : events) {
            // Intercept any event
            Event interceptedEvent = intercept(event);
            interceptedEvents.add(interceptedEvent);
        }

        return interceptedEvents;
    }

    public void close() {
        // At interceptor shutdown
    }

    /////////////////
    //   BUILDER   //
    /////////////////

    public static class Builder implements Interceptor.Builder {

        public void configure(Context context) {
            // Retrieve property from flume conf
            //hostHeader = context.getString("hostHeader");
        }
        public Interceptor build() {
            return new CustomHostInterceptor();
        }
    }

    public static Map<String, String> parse(JSONObject json, Map<String, String> out) throws JSONException {
        Iterator<String> keys = json.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            String val = null;
            try {
                JSONObject value = json.getJSONObject(key);
                parse(value, out);
            } catch (Exception e) {
                val = json.getString(key);
            }

            if (val != null) {
                out.put(key, val);
            }
        }
        return out;
    }
}

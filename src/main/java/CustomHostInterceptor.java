package com.semantix;

import java.util.*;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
//import org.json.JSONException;
//import org.json.simple.JSONObject;
//import org.json.simple.JSONArray;
//import org.json.simple.parser.ParseException;
//import org.json.simple.parser.JSONParser;
/**
 * Hello world!
 */
public class CustomHostInterceptor
        implements Interceptor {

    private String hostValue;
    private String hostHeader;

    public CustomHostInterceptor(String hostHeader){
        this.hostHeader = hostHeader;
    }


    public void initialize() {
        // At interceptor start up
	
	/*        
	try {
            hostValue =
                    InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new FlumeException("Cannot get Hostname", e);
        }
	*/
    }
    public Event intercept(Event event) {
        byte[] json = event.getBody();
//        it seems that tha content comes as an JSON object not an array
//        JSONArray jsonArray = new JSONArray(new String(json));
//        JSONObject object = jsonArray.getJSONObject(0);
        System.out.println("INFO JSON data: ");
        System.out.println(new String(json));
        System.out.println(event.toString());
        JSONObject object = new JSONObject(new String(json));
//        String flattenedJson = FlattenJSON(new String(eventBody));
        Map<String,String> out = new HashMap<String, String>();

        parse(object,out);
        String alarm_name = out.get("alarm_name");
        String object1 = out.get("object");
        String status = out.get("status");
        String startts = out.get("startts");
        String endts = out.get("endts");
        String urgency = out.get("urgency");
        String jsonCsv = alarm_name + "," + object1 + "," + status + "," + startts + "," + endts + "," + urgency;
        System.out.println(jsonCsv);
        event.setBody(jsonCsv.getBytes());
//        event.setBody(json);
        return event;
    	/*
 
        // This is the event's body
        String body = new String(event.getBody());
 
        // These are the event's headers
        Map<String, String> headers = event.getHeaders();
 
        // Enrich header with hostname
        headers.put(hostHeader, hostValue);
 
        // Let the enriched event go
        */
//        return event;
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

    public static class Builder
            implements Interceptor.Builder {

        private String hostHeader;

        public void configure(Context context) {
            // Retrieve property from flume conf
            //hostHeader = context.getString("hostHeader");
        }


        public Interceptor build() {
            return new CustomHostInterceptor(hostHeader);
        }
    }
    public static Map<String,String> parse(JSONObject json , Map<String,String> out) throws JSONException {
        Iterator<String> keys = json.keys();
        while(keys.hasNext()){
            String key = keys.next();
            String val = null;
            try{
                JSONObject value = json.getJSONObject(key);
                parse(value,out);
            }catch(Exception e){
                val = json.getString(key);
            }

            if(val != null){
                out.put(key,val);
            }
        }
        return out;
    }


    public static void main(String[] args){

//        JSONParser parser = new JSONParser();
        String json = "[{\"alarm_name\": \"alarme\", \"object\": \"objeto\", \"status\": \"1\", \"startts\": \"20161218\", \"endts\": \"20161219\", \"urgency\": \"critco\"}]" ;
        JSONArray jsonArray = new JSONArray(json);
        JSONObject object = jsonArray.getJSONObject(0);

//        JSONObject info = object.getJSONObject("ipinfo");

        Map<String,String> out = new HashMap<String, String>();

        parse(object,out);

        String alarm_name = out.get("alarm_name");
        String object1 = out.get("object");
        String status = out.get("status");
        String startts = out.get("startts");
        String endts = out.get("endts");
        String urgency = out.get("urgency");
        String jsonCsv = alarm_name + "," + object1 + "," + status + "," + startts + "," + endts + "," + urgency;
        System.out.println(jsonCsv);
        System.out.println("alarm_name : " + alarm_name + " object : " + object1 + " status : "+status + " Startts : "+ startts + " endts : "+endts+" urgency "+urgency);

        System.out.println("ALL VALUE " + out);

//        try{
//            Object obj = parser.parse(s);
//            JSONArray array = (JSONArray) obj;
//
//            System.out.println("The 2nd element of array");
//            System.out.println(array.get(0));
//            System.out.println();
//
//        }catch(Exception pe){
//
//            System.out.println("position: " + pe);
//            System.out.println(pe);
//        }
    }
}
package example;

import java.util.Map;

import apoc.result.VirtualRelationship;
import apoc.result.VirtualNode;
import org.neo4j.graphdb.Label;
import apoc.util.Util;
import java.util.ArrayList;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Relationship;
import java.util.LinkedList;
import org.neo4j.graphdb.Entity;
import apoc.result.GraphResult;
import apoc.result.NodeResult;
import apoc.result.RelationshipResult;

import java.util.List;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Node;
import org.neo4j.procedure.builtin.BuiltInProcedures;

import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.neo4j.procedure.Name;
import org.neo4j.logging.Log;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.procedure.Context;
import org.neo4j.graphdb.GraphDatabaseService;


public class Trajectories
{
    @Context
    public GraphDatabaseService db;
    @Context
    public GraphDatabaseAPI api;
    @Context
    public Transaction tx;
    @Context
    public Log log;
    
    @Procedure(mode = Mode.WRITE, value = "example.getHybridTrajectory")
    public Stream<BuiltInProcedures.NodeResult> getHybridTraj(@Name("participantId") final Long participantId, @Name("start time") final String startTime, @Name("end time") final String endTime) {
        String query = "MATCH (timeInterval:TimeInterval {participantID : toString(" + participantId + ") })-[:HAS_PLACE]->(poi:Place)-[r*]->(m) ";
        if (startTime != null && startTime != "") {
            query = String.valueOf(query) + " WHERE timeInterval.start > toString('" + startTime + "')";
        }
        if (endTime != null && endTime != "") {
            query = String.valueOf(query) + " AND timeInterval.end < toString('" + endTime + "')";
        }
        query = String.valueOf(query) + " RETURN * ORDER BY timeInterval.start";
        final Result result = this.tx.execute(query);
        this.log.warn(result.resultAsString());
        return result.stream().map(row -> new BuiltInProcedures.NodeResult((Node)row.get("poi")));
    }

    
    @Procedure(mode = Mode.WRITE, value = "example.getHybridTrajGraphAccordingToLayersInterest")
    public Stream<GraphResult> getHybridTrajGraphAccordingToLayersInterest(@Name("participantId") Long participantId, @Name("UserInterests") List<String> layersNames, @Name("start time") String startTime, @Name("end time")  String endTime) {
        String query = "MATCH (timeInterval:TimeInterval {participantID : toString(" + participantId + ") })-[:HAS_PLACE]->(p:Place)-[r*]->(m) ";
        if (startTime != null && startTime != "") {
            query = String.valueOf(query) + " WHERE timeInterval.start > toString('" + startTime + "')";
            
            if (endTime != null && endTime != "" ) {
                query = String.valueOf(query) + " AND timeInterval.end < toString('" + endTime + "')";
            }
        }
        else if (endTime != null && endTime != "" ) { // if there is only endtime
        	query = String.valueOf(query) + " WHERE timeInterval.end < toString('" + endTime + "')";
        }
        
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_NO2_SEMANTIC]->(no2:No2Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_PM10_SEMANTIC]->(pm10:Pm10Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_PM1_0_SEMANTIC]->(pm1:Pm1_0Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_PM2_5_SEMANTIC]->(pm2:Pm2_5Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_BC_SEMANTIC]->(bc:BCSemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_TEMPERATURE_SEMANTIC]->(temp:TemperatureSemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_HUMIDITY_SEMANTIC]->(hum:HumiditySemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_ACTIVITY_SEMANTIC]->(act:ActivitySemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_EVENT_SEMANTIC]->(eve:EventSemantic)";
        query = String.valueOf(query) + " RETURN * ORDER BY timeInterval.start";
        
        Result result = this.tx.execute(query);
        
        Entity[] entityTab = new Entity[100000];
        
        List<Node> servers = new LinkedList<Node>();
        List<Relationship> relationships = new LinkedList<Relationship>();
        
        int entityIndex = 0;
        Boolean poiFound = false;
        Node previousTimeInterval = null;
        
        VirtualNode fromNode = null;
        
        Boolean temporalAggregationIntervalExtended = false;
        //Node previous_place = place;
        //Node previous_m = m;
        Node previous_no2 = null;
        Node previous_pm10 = null;
        Node previous_pm11 = null;
        Node previous_pm12 = null;
        Node previous_bc = null;
        Node previous_temp = null;
        Node previous_hum = null;
        Node previous_act = null;
        Node previous_eve = null;
        
        VirtualNode previous_fromNode = null;
        
        while (result.hasNext()) {
            Map<String, Object> row = (Map<String, Object>)result.next();
            Node timeInterval = (Node) row.get("timeInterval");
            Node place = (Node) row.get("p");
            Node m = (Node) row.get("m");
            Node no2 = (Node) row.get("no2");
            Node pm10 = (Node) row.get("pm10");
            Node pm11 = (Node) row.get("pm1");
            Node pm12 = (Node) row.get("pm2");
            Node bc = (Node) row.get("bc");
            Node temp = (Node) row.get("temp");
            Node hum = (Node) row.get("hum");
            Node act = (Node) row.get("act");
            Node eve = (Node) row.get("eve");
            RelationshipType no2Relashionship = RelationshipType.withName("HAS_NO2_SEMANTIC");
            RelationshipType pm10Relashionship = RelationshipType.withName("HAS_PM10_SEMANTIC");
            RelationshipType pm1Relashionship = RelationshipType.withName("HAS_PM1_0_SEMANTIC");
            RelationshipType pm2tRelashionship = RelationshipType.withName("HAS_PM2_5_SEMANTIC");
            RelationshipType bcRelashionship = RelationshipType.withName("HAS_BC_SEMANTIC");
            RelationshipType tempRelashionship = RelationshipType.withName("HAS_TEMPERATURE_SEMANTIC");
            RelationshipType humRelashionship = RelationshipType.withName("HAS_HUMIDITY_SEMANTIC");
            RelationshipType actRelashionship = RelationshipType.withName("HAS_ACTIVITY_SEMANTIC");
            RelationshipType eveRelashionship = RelationshipType.withName("HAS_EVENT_SEMANTIC");
            
            
            if (poiFound) {
                if (previousTimeInterval.equals(timeInterval)) {
                    continue;
                }
                poiFound = false;
            }
            
            RelationshipType nextRelashionship = RelationshipType.withName("NEXT");
            
            List<String> labelPOIs = new ArrayList<String>();
            labelPOIs.add("POI");
            Label[] poiLabels = Util.labels((Object)labelPOIs);
            
            for (Label label : place.getLabels()) {
                for (String layer : layersNames) {
                    if (label.name().equals(layer)) {
                        fromNode = new VirtualNode(poiLabels, place.getAllProperties());
                        for (String key : timeInterval.getPropertyKeys()) {
                            fromNode.setProperty(key, timeInterval.getProperty(key));
                        }
                        fromNode.setProperty("layer", layer);
                        poiFound = true;
                        previousTimeInterval = timeInterval;
                        break;
                    }
                }
                if (poiFound) {
                    break;
                }
            }
            
            if (!poiFound) {
                for (Label label : m.getLabels()) {
                    for (String layer : layersNames) {
                        if (label.name().equals(layer)) {
                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
                            for (String key : timeInterval.getPropertyKeys()) {
                                fromNode.setProperty(key, timeInterval.getProperty(key));
                            }
                            fromNode.setProperty("layer", layer);
                            poiFound = true;
                            previousTimeInterval = timeInterval;
                            break;
                        }
                    }
                    if (poiFound) {
                        break;
                    }
                }
            }
                       
            if (poiFound) {
                if (entityIndex == 0) {
                    entityTab[entityIndex] = (Entity)fromNode;
                    ++entityIndex;
                    servers.add((Node)fromNode);
                    servers.add(no2);
                    servers.add(pm10);
                    servers.add(pm11);
                    servers.add(pm12);
                    servers.add(bc);
                    servers.add(temp);
                    servers.add(hum);
                    servers.add(act);
                    servers.add(eve);
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(no2, no2Relashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(pm10, pm10Relashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(pm11, pm1Relashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(pm12, pm2tRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(bc, bcRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(temp, tempRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(hum, humRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(act, actRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(eve, eveRelashionship));
                }
                else {
                	
                	if(previous_fromNode.getProperty("layer").equals(fromNode.getProperty("layer"))
                			&& previous_no2.equals(no2) && previous_pm10.equals(pm10) && previous_pm11.equals(pm11) && previous_pm12.equals(pm12) && previous_bc.equals(bc)
                			&& previous_temp.equals(temp) && previous_hum.equals(hum) && previous_act.equals(act) && previous_eve.equals(eve)) {//if previous node contextual values equal actual node and they are at same layer
                		
                		int index = servers.lastIndexOf((Node)previous_fromNode);
                		log.warn("INTERVAL EXTENDED");
                		log.warn(Integer.toString(index));
                		servers.get(index).setProperty("end", fromNode.getProperty("end")); //update end timeinterval for temporal aggregation of the segment
                		temporalAggregationIntervalExtended = true;                		        		
                	}
                	else {
                		entityTab[entityIndex + 1] = (Entity)fromNode;
                        servers.add((Node)(entityTab[entityIndex + 1]));
                        servers.add(no2);
                        servers.add(pm10);
                        servers.add(pm11);
                        servers.add(pm12);
                        servers.add(bc);
                        servers.add(temp);
                        servers.add(hum);
                        servers.add(act);
                        servers.add(eve);
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(no2, no2Relashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(pm10, pm10Relashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(pm11, pm1Relashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(pm12, pm2tRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(bc, bcRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(temp, tempRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(hum, humRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(act, actRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(eve, eveRelashionship));
                        final VirtualRelationship vrel = (VirtualRelationship)((Node)entityTab[entityIndex - 1]).createRelationshipTo((Node)entityTab[entityIndex + 1], nextRelashionship);
                        entityTab[entityIndex] = (Entity)vrel;
                        relationships.add((Relationship)(entityTab[entityIndex]));
                        entityIndex += 2;
                        
                        temporalAggregationIntervalExtended = false;
                	}       	
                }
                if(!temporalAggregationIntervalExtended) {//save previous poi node and contextual nodes if the interval not extended. Otherwise do not save it because already saved and to prevent an error               	
                    previous_no2 = no2;
                    previous_pm10 = pm10;
                    previous_pm11 = pm11;
                    previous_pm12 = pm12;
                    previous_bc = bc;
                    previous_temp = temp;
                    previous_hum = hum;
                    previous_act = act;
                    previous_eve = eve;               
                    previous_fromNode = fromNode;   
                }                 
            }
            else {
                log.warn("print final poiFound");
                log.warn(poiFound.toString());
            }
        }
        log.warn("Finish or what??????????????????????????????????????????????????????????????????????");
        for (int i = 0; i < entityIndex; ++i) {
            if (i % 2 != 0) {
                RelationshipType relt = ((Relationship)entityTab[i]).getType();
                log.warn("Relationship:");
                log.warn(relt.name());
            }
            else {
                Iterable<Label> l = (Iterable<Label>)((Node)entityTab[i]).getLabels();
                log.warn("Node:");
                for (Label li : l) {
                    log.warn(li.name());
                }
            }
            log.warn(Integer.toString(i));
        }
        GraphResult graphResult = new GraphResult(servers, relationships);
        createGraph(relationships); //relationships contains starts and end nodes also
        
        return Stream.of(graphResult);
    }

    
    @Procedure(mode = Mode.WRITE, value = "example.getHybridTrajGraphAccordingToCategoryInterest")
    public Stream<GraphResult> getHybridTrajGraphAccordingToCategoryInterest(
    		@Name("participantId") Long participantId, 
    		@Name("CategoryInterests") List<String> categoriesNames, 
    		@Name("finerLayerInterest") String finerLayerName, 
    		@Name("coarserLayerInterest") String coarserLayerName, 
    		@Name("start time") String startTime, 
    		@Name("end time")  String endTime) {
    	
    	String query = "MATCH (timeInterval:TimeInterval {participantID : toString(" + participantId + ") })-[:HAS_PLACE]->(poi:Place)-[r*]->(m) ";
    	if (startTime != null && startTime != "") {
            query = String.valueOf(query) + " WHERE timeInterval.start > toString('" + startTime + "')";
            
            if (endTime != null && endTime != "" ) {
                query = String.valueOf(query) + " AND timeInterval.end < toString('" + endTime + "')";
            }
        }
        else if (endTime != null && endTime != "" ) { // if there is only endtime
        	query = String.valueOf(query) + " WHERE timeInterval.end < toString('" + endTime + "')";
        }
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_NO2_SEMANTIC]->(no2:No2Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_PM10_SEMANTIC]->(pm10:Pm10Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_PM1_0_SEMANTIC]->(pm1:Pm1_0Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_PM2_5_SEMANTIC]->(pm2:Pm2_5Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_BC_SEMANTIC]->(bc:BCSemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_TEMPERATURE_SEMANTIC]->(temp:TemperatureSemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_HUMIDITY_SEMANTIC]->(hum:HumiditySemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_ACTIVITY_SEMANTIC]->(act:ActivitySemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_EVENT_SEMANTIC]->(eve:EventSemantic)";
        query = String.valueOf(query) + " RETURN * ORDER BY timeInterval.start";
        
        Result result = this.tx.execute(query);
        
        Entity[] entityTab = new Entity[100000];
        
        List<Node> servers = new LinkedList<Node>();
        List<Relationship> relationships = new LinkedList<Relationship>();
        
        int entityIndex = 0;
        Boolean poiFound = false;
        Node previousTimeInterval = null;
        
        VirtualNode fromNode = null;
        
        Boolean temporalAggregationIntervalExtended = false;
        //Node previous_place = place;
        //Node previous_m = m;
        Node previous_no2 = null;
        Node previous_pm10 = null;
        Node previous_pm11 = null;
        Node previous_pm12 = null;
        Node previous_bc = null;
        Node previous_temp = null;
        Node previous_hum = null;
        Node previous_act = null;
        Node previous_eve = null;
        
        VirtualNode previous_fromNode = null;
        
        while (result.hasNext()) {
            Map<String, Object> row = (Map<String, Object>)result.next();
            Node timeInterval = (Node) row.get("timeInterval");
            Node poi = (Node) row.get("poi");
            Node m = (Node) row.get("m");
            Node no2 = (Node) row.get("no2");
            Node pm10 = (Node) row.get("pm10");
            Node pm11 = (Node) row.get("pm1");
            Node pm12 = (Node) row.get("pm2");
            Node bc = (Node) row.get("bc");
            Node temp = (Node) row.get("temp");
            Node hum = (Node) row.get("hum");
            Node act = (Node) row.get("act");
            Node eve = (Node) row.get("eve");
            RelationshipType no2Relashionship = RelationshipType.withName("HAS_NO2_SEMANTIC");
            RelationshipType pm10Relashionship = RelationshipType.withName("HAS_PM10_SEMANTIC");
            RelationshipType pm1Relashionship = RelationshipType.withName("HAS_PM1_0_SEMANTIC");
            RelationshipType pm2tRelashionship = RelationshipType.withName("HAS_PM2_5_SEMANTIC");
            RelationshipType bcRelashionship = RelationshipType.withName("HAS_BC_SEMANTIC");
            RelationshipType tempRelashionship = RelationshipType.withName("HAS_TEMPERATURE_SEMANTIC");
            RelationshipType humRelashionship = RelationshipType.withName("HAS_HUMIDITY_SEMANTIC");
            RelationshipType actRelashionship = RelationshipType.withName("HAS_ACTIVITY_SEMANTIC");
            RelationshipType eveRelashionship = RelationshipType.withName("HAS_EVENT_SEMANTIC");
            
            
            if (poiFound) {
                if (previousTimeInterval.equals(timeInterval)) {
                    continue;
                }
                poiFound = false;
            }
            
            RelationshipType nextRelashionship = RelationshipType.withName("NEXT");
            
            List<String> labelPOIs = new ArrayList<String>();
            labelPOIs.add("POI");
            Label[] poiLabels = Util.labels((Object)labelPOIs);
            
            if(poi.hasProperty("category") && categoriesNames.contains(poi.getProperty("category"))) {       //when find condition for finer Layer of POI 	
            	for (Label label : poi.getLabels()) {              	
                    if (label.name().equals(finerLayerName)) {
                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
                        for (String key : timeInterval.getPropertyKeys()) {
                            fromNode.setProperty(key, timeInterval.getProperty(key));
                        }
                        fromNode.setProperty("layer", finerLayerName);
                        poiFound = true;
                        previousTimeInterval = timeInterval;
                        break;
                    }
                    if (poiFound) {
                        break;
                    }              
                }
                
                if (!poiFound) {
                    for (Label label : m.getLabels()) {              	
                        if (label.name().equals(finerLayerName)) {
                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
                            for (String key : timeInterval.getPropertyKeys()) {
                                fromNode.setProperty(key, timeInterval.getProperty(key));
                            }
                            fromNode.setProperty("layer", finerLayerName);
                            poiFound = true;
                            previousTimeInterval = timeInterval;
                            break;
                        }
                        if (poiFound) {
                            break;
                        }
                    }
                }
            } 
            else { //when not found the condition for finer Layer of POI then reach coarser Layer of POI 	
            	for (Label label : poi.getLabels()) {              	
                    if (label.name().equals(coarserLayerName)) {
                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
                        for (String key : timeInterval.getPropertyKeys()) {
                            fromNode.setProperty(key, timeInterval.getProperty(key));
                        }
                        fromNode.setProperty("layer", coarserLayerName);
                        poiFound = true;
                        previousTimeInterval = timeInterval;
                        break;
                    }
                    if (poiFound) {
                        break;
                    }              
                }
                
                if (!poiFound) {
                    for (Label label : m.getLabels()) {              	
                        if (label.name().equals(coarserLayerName)) {
                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
                            for (String key : timeInterval.getPropertyKeys()) {
                                fromNode.setProperty(key, timeInterval.getProperty(key));
                            }
                            fromNode.setProperty("layer", coarserLayerName);
                            poiFound = true;
                            previousTimeInterval = timeInterval;
                            break;
                        }
                        if (poiFound) {
                            break;
                        }
                    }
                }
            }          
            
            if (poiFound) {
                if (entityIndex == 0) {
                    entityTab[entityIndex] = (Entity)fromNode;
                    ++entityIndex;
                    servers.add((Node)fromNode);
                    servers.add(no2);
                    servers.add(pm10);
                    servers.add(pm11);
                    servers.add(pm12);
                    servers.add(bc);
                    servers.add(temp);
                    servers.add(hum);
                    servers.add(act);
                    servers.add(eve);
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(no2, no2Relashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(pm10, pm10Relashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(pm11, pm1Relashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(pm12, pm2tRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(bc, bcRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(temp, tempRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(hum, humRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(act, actRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(eve, eveRelashionship));
                }
                else {
                	
                	if(previous_fromNode.getProperty("layer").equals(fromNode.getProperty("layer"))
                			&& previous_no2.equals(no2) && previous_pm10.equals(pm10) && previous_pm11.equals(pm11) && previous_pm12.equals(pm12) && previous_bc.equals(bc)
                			&& previous_temp.equals(temp) && previous_hum.equals(hum) && previous_act.equals(act) && previous_eve.equals(eve)) {//if previous node contextual values equal actual node and they are at same layer
                		
                		int index = servers.lastIndexOf((Node)previous_fromNode);
                		log.warn("INTERVAL EXTENDED");
                		log.warn(Integer.toString(index));
                		servers.get(index).setProperty("end", fromNode.getProperty("end")); //update end timeinterval for temporal aggregation of the segment
                		temporalAggregationIntervalExtended = true;                		        		
                	}
                	else {
                		entityTab[entityIndex + 1] = (Entity)fromNode;
                        servers.add((Node)(entityTab[entityIndex + 1]));
                        servers.add(no2);
                        servers.add(pm10);
                        servers.add(pm11);
                        servers.add(pm12);
                        servers.add(bc);
                        servers.add(temp);
                        servers.add(hum);
                        servers.add(act);
                        servers.add(eve);
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(no2, no2Relashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(pm10, pm10Relashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(pm11, pm1Relashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(pm12, pm2tRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(bc, bcRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(temp, tempRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(hum, humRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(act, actRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(eve, eveRelashionship));
                        final VirtualRelationship vrel = (VirtualRelationship)((Node)entityTab[entityIndex - 1]).createRelationshipTo((Node)entityTab[entityIndex + 1], nextRelashionship);
                        entityTab[entityIndex] = (Entity)vrel;
                        relationships.add((Relationship)(entityTab[entityIndex]));
                        entityIndex += 2;
                        
                        temporalAggregationIntervalExtended = false;
                	}       	
                }
                if(!temporalAggregationIntervalExtended) {//save previous poi node and contextual nodes if the interval not extended. Otherwise do not save it because already saved and to prevent an error               	
                    previous_no2 = no2;
                    previous_pm10 = pm10;
                    previous_pm11 = pm11;
                    previous_pm12 = pm12;
                    previous_bc = bc;
                    previous_temp = temp;
                    previous_hum = hum;
                    previous_act = act;
                    previous_eve = eve;               
                    previous_fromNode = fromNode;   
                }                 
            }
            else {
                log.warn("print final poiFound");
                log.warn(poiFound.toString());
            }
        }
        log.warn("Finish or what??????????????????????????????????????????????????????????????????????");
        for (int i = 0; i < entityIndex; ++i) {
            if (i % 2 != 0) {
                RelationshipType relt = ((Relationship)entityTab[i]).getType();
                log.warn("Relationship:");
                log.warn(relt.name());
            }
            else {
                Iterable<Label> l = (Iterable<Label>)((Node)entityTab[i]).getLabels();
                log.warn("Node:");
                for (Label li : l) {
                    log.warn(li.name());
                }
            }
            log.warn(Integer.toString(i));
        }
        GraphResult graphResult = new GraphResult(servers, relationships);
        createGraph(relationships); //relationships contains starts and end nodes alsos
        
        return Stream.of(graphResult);
    }
   
    
    @Procedure(mode = Mode.WRITE, value = "example.getHybridTrajGraphAccordingToPlacePropertyValueInterest")
    public Stream<GraphResult> getHybridTrajGraphAccordingToPlacePropertyValueInterest(
    		@Name("participantId") Long participantId, 
    		@Name("contextValueInterestsMap") Map<String, String> contextValueMap, 
    		@Name("finerLayerInterest") String finerLayerName, 
    		@Name("coarserLayerInterest") String coarserLayerName, 
    		@Name("start time") String startTime, 
    		@Name("end time")  String endTime) {
    	
    	
    	String query = "MATCH (timeInterval:TimeInterval {participantID : toString(" + participantId + ") })-[:HAS_PLACE]->(poi:Place)-[r*]->(m) ";
    	if (startTime != null && startTime != "") {
            query = String.valueOf(query) + " WHERE timeInterval.start > toString('" + startTime + "')";
            
            if (endTime != null && endTime != "" ) {
                query = String.valueOf(query) + " AND timeInterval.end < toString('" + endTime + "')";
            }
        }
        else if (endTime != null && endTime != "" ) { // if there is only endtime
        	query = String.valueOf(query) + " WHERE timeInterval.end < toString('" + endTime + "')";
        }
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_NO2_SEMANTIC]->(no2:No2Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_PM10_SEMANTIC]->(pm10:Pm10Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_PM1_0_SEMANTIC]->(pm1:Pm1_0Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_PM2_5_SEMANTIC]->(pm2:Pm2_5Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_BC_SEMANTIC]->(bc:BCSemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_TEMPERATURE_SEMANTIC]->(temp:TemperatureSemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_HUMIDITY_SEMANTIC]->(hum:HumiditySemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_ACTIVITY_SEMANTIC]->(act:ActivitySemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_EVENT_SEMANTIC]->(eve:EventSemantic)";
        query = String.valueOf(query) + " RETURN * ORDER BY timeInterval.start";
        
        Result result = this.tx.execute(query);
        
        Entity[] entityTab = new Entity[100000];
        
        List<Node> servers = new LinkedList<Node>();
        List<Relationship> relationships = new LinkedList<Relationship>();
        
        int entityIndex = 0;
        Boolean poiFound = false;
        Node previousTimeInterval = null;
        
        VirtualNode fromNode = null;
        
        Boolean temporalAggregationIntervalExtended = false;
        //Node previous_place = place;
        //Node previous_m = m;
        Node previous_no2 = null;
        Node previous_pm10 = null;
        Node previous_pm11 = null;
        Node previous_pm12 = null;
        Node previous_bc = null;
        Node previous_temp = null;
        Node previous_hum = null;
        Node previous_act = null;
        Node previous_eve = null;
        
        VirtualNode previous_fromNode = null;
        
        while (result.hasNext()) {
            Map<String, Object> row = (Map<String, Object>)result.next();
            Node timeInterval = (Node) row.get("timeInterval");
            Node poi = (Node) row.get("poi");
            Node m = (Node) row.get("m");
            Node no2 = (Node) row.get("no2");
            Node pm10 = (Node) row.get("pm10");
            Node pm11 = (Node) row.get("pm1");
            Node pm12 = (Node) row.get("pm2");
            Node bc = (Node) row.get("bc");
            Node temp = (Node) row.get("temp");
            Node hum = (Node) row.get("hum");
            Node act = (Node) row.get("act");
            Node eve = (Node) row.get("eve");
            RelationshipType no2Relashionship = RelationshipType.withName("HAS_NO2_SEMANTIC");
            RelationshipType pm10Relashionship = RelationshipType.withName("HAS_PM10_SEMANTIC");
            RelationshipType pm1Relashionship = RelationshipType.withName("HAS_PM1_0_SEMANTIC");
            RelationshipType pm2tRelashionship = RelationshipType.withName("HAS_PM2_5_SEMANTIC");
            RelationshipType bcRelashionship = RelationshipType.withName("HAS_BC_SEMANTIC");
            RelationshipType tempRelashionship = RelationshipType.withName("HAS_TEMPERATURE_SEMANTIC");
            RelationshipType humRelashionship = RelationshipType.withName("HAS_HUMIDITY_SEMANTIC");
            RelationshipType actRelashionship = RelationshipType.withName("HAS_ACTIVITY_SEMANTIC");
            RelationshipType eveRelashionship = RelationshipType.withName("HAS_EVENT_SEMANTIC");
            
            
            if (poiFound) {
                if (previousTimeInterval.equals(timeInterval)) {
                    continue;
                }
                poiFound = false;
            }
            
            
            RelationshipType nextRelashionship = RelationshipType.withName("NEXT");
            
            List<String> labelPOIs = new ArrayList<String>();
            labelPOIs.add("POI");
            Label[] poiLabels = Util.labels((Object)labelPOIs);
            
            
            
            for (Map.Entry<String, String> entry : contextValueMap.entrySet()) {
                //System.out.println(entry.getKey() + "/" + entry.getValue());
            	
            	
            	String propertyKey = ((String)entry.getKey()).toString(); //get the string from the entered property name
            
	            if(poi.hasProperty(propertyKey) && poi.getProperty(propertyKey).equals(entry.getValue()) ) {       //when find condition for finer Layer of POI 	
	            	for (Label label : poi.getLabels()) {              	
	                    if (label.name().equals(finerLayerName)) {
	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	                        for (String key : timeInterval.getPropertyKeys()) {
	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	                        }
	                        fromNode.setProperty("layer", finerLayerName);
	                        poiFound = true;
	                        previousTimeInterval = timeInterval;
	                        break;
	                    }
	                    if (poiFound) {
	                        break;
	                    }              
	                }
	                
	                if (!poiFound) {
	                    for (Label label : m.getLabels()) {              	
	                        if (label.name().equals(finerLayerName)) {
	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	                            for (String key : timeInterval.getPropertyKeys()) {
	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	                            }
	                            fromNode.setProperty("layer", finerLayerName);
	                            poiFound = true;
	                            previousTimeInterval = timeInterval;
	                            break;
	                        }
	                        if (poiFound) {
	                            break;
	                        }
	                    }
	                }
	            } 
	            else { //when not found the condition for finer Layer of POI then reach coarser Layer of POI 	
	            	for (Label label : poi.getLabels()) {              	
	                    if (label.name().equals(coarserLayerName)) {
	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	                        for (String key : timeInterval.getPropertyKeys()) {
	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	                        }
	                        fromNode.setProperty("layer", coarserLayerName);
	                        poiFound = true;
	                        previousTimeInterval = timeInterval;
	                        break;
	                    }
	                    if (poiFound) {
	                        break;
	                    }              
	                }
	                
	                if (!poiFound) {
	                    for (Label label : m.getLabels()) {              	
	                        if (label.name().equals(coarserLayerName)) {
	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	                            for (String key : timeInterval.getPropertyKeys()) {
	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	                            }
	                            fromNode.setProperty("layer", coarserLayerName);
	                            poiFound = true;
	                            previousTimeInterval = timeInterval;
	                            break;
	                        }
	                        if (poiFound) {
	                            break;
	                        }
	                    }
	                }
	            }
            
            }
            
            if (poiFound) {
                if (entityIndex == 0) {
                    entityTab[entityIndex] = (Entity)fromNode;
                    ++entityIndex;
                    servers.add((Node)fromNode);
                    servers.add(no2);
                    servers.add(pm10);
                    servers.add(pm11);
                    servers.add(pm12);
                    servers.add(bc);
                    servers.add(temp);
                    servers.add(hum);
                    servers.add(act);
                    servers.add(eve);
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(no2, no2Relashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(pm10, pm10Relashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(pm11, pm1Relashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(pm12, pm2tRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(bc, bcRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(temp, tempRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(hum, humRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(act, actRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(eve, eveRelashionship));
                }
                else {
                	
                	if(previous_fromNode.getProperty("layer").equals(fromNode.getProperty("layer"))
                			&& previous_no2.equals(no2) && previous_pm10.equals(pm10) && previous_pm11.equals(pm11) && previous_pm12.equals(pm12) && previous_bc.equals(bc)
                			&& previous_temp.equals(temp) && previous_hum.equals(hum) && previous_act.equals(act) && previous_eve.equals(eve)) {//if previous node contextual values equal actual node and they are at same layer
                		
                		int index = servers.lastIndexOf((Node)previous_fromNode);
                		log.warn("INTERVAL EXTENDED");
                		log.warn(Integer.toString(index));
                		servers.get(index).setProperty("end", fromNode.getProperty("end")); //update end timeinterval for temporal aggregation of the segment
                		temporalAggregationIntervalExtended = true;                		        		
                	}
                	else {
                		entityTab[entityIndex + 1] = (Entity)fromNode;
                        servers.add((Node)(entityTab[entityIndex + 1]));
                        servers.add(no2);
                        servers.add(pm10);
                        servers.add(pm11);
                        servers.add(pm12);
                        servers.add(bc);
                        servers.add(temp);
                        servers.add(hum);
                        servers.add(act);
                        servers.add(eve);
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(no2, no2Relashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(pm10, pm10Relashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(pm11, pm1Relashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(pm12, pm2tRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(bc, bcRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(temp, tempRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(hum, humRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(act, actRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(eve, eveRelashionship));
                        final VirtualRelationship vrel = (VirtualRelationship)((Node)entityTab[entityIndex - 1]).createRelationshipTo((Node)entityTab[entityIndex + 1], nextRelashionship);
                        entityTab[entityIndex] = (Entity)vrel;
                        relationships.add((Relationship)(entityTab[entityIndex]));
                        entityIndex += 2;
                        
                        temporalAggregationIntervalExtended = false;
                	}       	
                }
                if(!temporalAggregationIntervalExtended) {//save previous poi node and contextual nodes if the interval not extended. Otherwise do not save it because already saved and to prevent an error               	
                    previous_no2 = no2;
                    previous_pm10 = pm10;
                    previous_pm11 = pm11;
                    previous_pm12 = pm12;
                    previous_bc = bc;
                    previous_temp = temp;
                    previous_hum = hum;
                    previous_act = act;
                    previous_eve = eve;               
                    previous_fromNode = fromNode;   
                }                 
            }
            else {
                log.warn("print final poiFound");
                log.warn(poiFound.toString());
            }
        }
        log.warn("Finish or what??????????????????????????????????????????????????????????????????????");
        for (int i = 0; i < entityIndex; ++i) {
            if (i % 2 != 0) {
                RelationshipType relt = ((Relationship)entityTab[i]).getType();
                log.warn("Relationship:");
                log.warn(relt.name());
            }
            else {
                Iterable<Label> l = (Iterable<Label>)((Node)entityTab[i]).getLabels();
                log.warn("Node:");
                for (Label li : l) {
                    log.warn(li.name());
                }
            }
            log.warn(Integer.toString(i));
        }
    	
    	GraphResult graphResult = new GraphResult(servers, relationships);
    	createGraph(relationships); //relationships contains starts and end nodes also
    	
        return Stream.of(graphResult);
    	
    }
            
    
    @Procedure(mode = Mode.WRITE, value = "example.getHybridTrajGraphAccordingToContextValueInterest")
    public Stream<GraphResult> getHybridTrajGraphAccordingToContextValueInterest(
    		@Name("participantId") Long participantId,
    		@Name("semanticNodeLabelName") String semanticNodeLabelName,
    		@Name("contextValueInterestsMap") Map<String, String> contextValueMap, 
    		@Name("finerLayerInterest") String finerLayerName, 
    		@Name("coarserLayerInterest") String coarserLayerName, 
    		@Name("start time") String startTime, 
    		@Name("end time")  String endTime) {
    	
    	String query = "MATCH (timeInterval:TimeInterval {participantID : toString(" + participantId + ") })-[:HAS_PLACE]->(poi:Place)-[r*]->(m) ";
    	if (startTime != null && startTime != "") {
            query = String.valueOf(query) + " WHERE timeInterval.start > toString('" + startTime + "')";
            
            if (endTime != null && endTime != "" ) {
                query = String.valueOf(query) + " AND timeInterval.end < toString('" + endTime + "')";
            }
        }
        else if (endTime != null && endTime != "" ) { // if there is only endtime
        	query = String.valueOf(query) + " WHERE timeInterval.end < toString('" + endTime + "')";
        }
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_NO2_SEMANTIC]->(no2:No2Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_PM10_SEMANTIC]->(pm10:Pm10Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_PM1_0_SEMANTIC]->(pm1:Pm1_0Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_PM2_5_SEMANTIC]->(pm2:Pm2_5Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_BC_SEMANTIC]->(bc:BCSemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_TEMPERATURE_SEMANTIC]->(temp:TemperatureSemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_HUMIDITY_SEMANTIC]->(hum:HumiditySemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_ACTIVITY_SEMANTIC]->(act:ActivitySemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_EVENT_SEMANTIC]->(eve:EventSemantic)";
        query = String.valueOf(query) + " RETURN * ORDER BY timeInterval.start";
        
        Result result = this.tx.execute(query);
        
        Entity[] entityTab = new Entity[100000];
        
        List<Node> servers = new LinkedList<Node>();
        List<Relationship> relationships = new LinkedList<Relationship>();
        
        int entityIndex = 0;
        Boolean poiFound = false;
        Node previousTimeInterval = null;
        
        VirtualNode fromNode = null;
        
        Boolean temporalAggregationIntervalExtended = false;
        //Node previous_place = place;
        //Node previous_m = m;
        Node previous_no2 = null;
        Node previous_pm10 = null;
        Node previous_pm11 = null;
        Node previous_pm12 = null;
        Node previous_bc = null;
        Node previous_temp = null;
        Node previous_hum = null;
        Node previous_act = null;
        Node previous_eve = null;
        
        VirtualNode previous_fromNode = null;
        
        while (result.hasNext()) {
            Map<String, Object> row = (Map<String, Object>)result.next();
            Node timeInterval = (Node) row.get("timeInterval");
            Node poi = (Node) row.get("poi");
            Node m = (Node) row.get("m");
            Node no2 = (Node) row.get("no2");
            Node pm10 = (Node) row.get("pm10");
            Node pm11 = (Node) row.get("pm1");
            Node pm12 = (Node) row.get("pm2");
            Node bc = (Node) row.get("bc");
            Node temp = (Node) row.get("temp");
            Node hum = (Node) row.get("hum");
            Node act = (Node) row.get("act");
            Node eve = (Node) row.get("eve");
            RelationshipType no2Relashionship = RelationshipType.withName("HAS_NO2_SEMANTIC");
            RelationshipType pm10Relashionship = RelationshipType.withName("HAS_PM10_SEMANTIC");
            RelationshipType pm1Relashionship = RelationshipType.withName("HAS_PM1_0_SEMANTIC");
            RelationshipType pm2tRelashionship = RelationshipType.withName("HAS_PM2_5_SEMANTIC");
            RelationshipType bcRelashionship = RelationshipType.withName("HAS_BC_SEMANTIC");
            RelationshipType tempRelashionship = RelationshipType.withName("HAS_TEMPERATURE_SEMANTIC");
            RelationshipType humRelashionship = RelationshipType.withName("HAS_HUMIDITY_SEMANTIC");
            RelationshipType actRelashionship = RelationshipType.withName("HAS_ACTIVITY_SEMANTIC");
            RelationshipType eveRelashionship = RelationshipType.withName("HAS_EVENT_SEMANTIC");           
                      
            if (poiFound) {
                if (previousTimeInterval.equals(timeInterval)) {
                    continue;
                }
                poiFound = false;
            }
           
            RelationshipType nextRelashionship = RelationshipType.withName("NEXT");
            
            List<String> labelPOIs = new ArrayList<String>();
            labelPOIs.add("POI");
            Label[] poiLabels = Util.labels((Object)labelPOIs);
            
            
            
            for (Map.Entry<String, String> entry : contextValueMap.entrySet()) {
                //System.out.println(entry.getKey() + "/" + entry.getValue());
            	
            	String propertyKey = ((String)entry.getKey()).toString(); //get the string from the entered property name     
            	
            	switch(semanticNodeLabelName) {
            	  case "No2Semantic":      
	      	            if(no2.hasProperty(propertyKey) && no2.getProperty(propertyKey).equals(entry.getValue()) ) {       //when find condition for finer Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(finerLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", finerLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(finerLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", finerLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            } 
	      	            else { //when not found the condition for finer Layer of POI then reach coarser Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(coarserLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", coarserLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(coarserLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", coarserLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            }	                  	                  
            		  break;
            		   
            	  case "Pm10Semantic":
            		  if(pm10.hasProperty(propertyKey) && pm10.getProperty(propertyKey).equals(entry.getValue()) ) {       //when find condition for finer Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(finerLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(finerLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", finerLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            } 
	      	            else { //when not found the condition for finer Layer of POI then reach coarser Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(coarserLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", coarserLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(coarserLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", coarserLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            }
            		  break;
            		   
            	  case "Pm1_0Semantic":
            		  if(pm11.hasProperty(propertyKey) && pm11.getProperty(propertyKey).equals(entry.getValue()) ) {       //when find condition for finer Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(finerLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", finerLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(finerLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", finerLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            } 
	      	            else { //when not found the condition for finer Layer of POI then reach coarser Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(coarserLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", coarserLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(coarserLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", coarserLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            }
            		  break;
            		  
            	  case "Pm2_5Semantic":
            		  if(pm12.hasProperty(propertyKey) && pm12.getProperty(propertyKey).equals(entry.getValue()) ) {       //when find condition for finer Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(finerLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", finerLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(finerLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", finerLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            } 
	      	            else { //when not found the condition for finer Layer of POI then reach coarser Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(coarserLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", coarserLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(coarserLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", coarserLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            }
            		  break;
            		   
            	  case "BCSemantic":
            		  if(bc.hasProperty(propertyKey) && bc.getProperty(propertyKey).equals(entry.getValue()) ) {       //when find condition for finer Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(finerLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", finerLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(finerLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", finerLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            } 
	      	            else { //when not found the condition for finer Layer of POI then reach coarser Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(coarserLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", coarserLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(coarserLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", coarserLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            }
            		  break;
            		   
            	  case "TemperatureSemantic":
            		  if(temp.hasProperty(propertyKey) && temp.getProperty(propertyKey).equals(entry.getValue()) ) {       //when find condition for finer Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(finerLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", finerLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(finerLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", finerLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            } 
	      	            else { //when not found the condition for finer Layer of POI then reach coarser Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(coarserLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", coarserLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(coarserLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", coarserLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            }
            		  break;
            		   
            	  case "HumiditySemantic":
            		  if(hum.hasProperty(propertyKey) && hum.getProperty(propertyKey).equals(entry.getValue()) ) {       //when find condition for finer Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(finerLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", finerLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(finerLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", finerLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            } 
	      	            else { //when not found the condition for finer Layer of POI then reach coarser Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(coarserLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", coarserLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(coarserLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", coarserLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            }
            		  break;
            		   
            	  case "ActivitySemantic":
            		  if(act.hasProperty(propertyKey) && act.getProperty(propertyKey).equals(entry.getValue()) ) {       //when find condition for finer Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(finerLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", finerLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(finerLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", finerLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            } 
	      	            else { //when not found the condition for finer Layer of POI then reach coarser Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(coarserLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", coarserLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(coarserLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", coarserLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            }
            		  break;
            		  
            	  case "EventSemantic":
            		  if(eve.hasProperty(propertyKey) && eve.getProperty(propertyKey).equals(entry.getValue()) ) {       //when find condition for finer Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(finerLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", finerLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(finerLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", finerLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            } 
	      	            else { //when not found the condition for finer Layer of POI then reach coarser Layer of POI 	
	      	            	for (Label label : poi.getLabels()) {              	
	      	                    if (label.name().equals(coarserLayerName)) {
	      	                        fromNode = new VirtualNode(poiLabels, poi.getAllProperties());
	      	                        for (String key : timeInterval.getPropertyKeys()) {
	      	                            fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                        }
	      	                        fromNode.setProperty("layer", coarserLayerName);
	      	                        poiFound = true;
	      	                        previousTimeInterval = timeInterval;
	      	                        break;
	      	                    }
	      	                    if (poiFound) {
	      	                        break;
	      	                    }              
	      	                }
	      	                
	      	                if (!poiFound) {
	      	                    for (Label label : m.getLabels()) {              	
	      	                        if (label.name().equals(coarserLayerName)) {
	      	                            fromNode = new VirtualNode(poiLabels, m.getAllProperties());
	      	                            for (String key : timeInterval.getPropertyKeys()) {
	      	                                fromNode.setProperty(key, timeInterval.getProperty(key));
	      	                            }
	      	                            fromNode.setProperty("layer", coarserLayerName);
	      	                            poiFound = true;
	      	                            previousTimeInterval = timeInterval;
	      	                            break;
	      	                        }
	      	                        if (poiFound) {
	      	                            break;
	      	                        }
	      	                    }
	      	                }
	      	            }
            		  break;             	    
            	  
            	  default: break;
            	    
            	}           	
            }
            	
            
            if (poiFound) {
                if (entityIndex == 0) {
                    entityTab[entityIndex] = (Entity)fromNode;
                    ++entityIndex;
                    servers.add((Node)fromNode);
                    servers.add(no2);
                    servers.add(pm10);
                    servers.add(pm11);
                    servers.add(pm12);
                    servers.add(bc);
                    servers.add(temp);
                    servers.add(hum);
                    servers.add(act);
                    servers.add(eve);
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(no2, no2Relashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(pm10, pm10Relashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(pm11, pm1Relashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(pm12, pm2tRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(bc, bcRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(temp, tempRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(hum, humRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(act, actRelashionship));
                    relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(eve, eveRelashionship));
                }
                else {
                	
                	if(previous_fromNode.getProperty("layer").equals(fromNode.getProperty("layer"))
                			&& previous_no2.equals(no2) && previous_pm10.equals(pm10) && previous_pm11.equals(pm11) && previous_pm12.equals(pm12) && previous_bc.equals(bc)
                			&& previous_temp.equals(temp) && previous_hum.equals(hum) && previous_act.equals(act) && previous_eve.equals(eve)) {//if previous node contextual values equal actual node and they are at same layer
                		
                		int index = servers.lastIndexOf((Node)previous_fromNode);
                		log.warn("INTERVAL EXTENDED");
                		log.warn(Integer.toString(index));
                		servers.get(index).setProperty("end", fromNode.getProperty("end")); //update end timeinterval for temporal aggregation of the segment
                		temporalAggregationIntervalExtended = true;                		        		
                	}
                	else {
                		entityTab[entityIndex + 1] = (Entity)fromNode;
                        servers.add((Node)(entityTab[entityIndex + 1]));
                        servers.add(no2);
                        servers.add(pm10);
                        servers.add(pm11);
                        servers.add(pm12);
                        servers.add(bc);
                        servers.add(temp);
                        servers.add(hum);
                        servers.add(act);
                        servers.add(eve);
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(no2, no2Relashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(pm10, pm10Relashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(pm11, pm1Relashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(pm12, pm2tRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(bc, bcRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(temp, tempRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(hum, humRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(act, actRelashionship));
                        relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(eve, eveRelashionship));
                        final VirtualRelationship vrel = (VirtualRelationship)((Node)entityTab[entityIndex - 1]).createRelationshipTo((Node)entityTab[entityIndex + 1], nextRelashionship);
                        entityTab[entityIndex] = (Entity)vrel;
                        relationships.add((Relationship)(entityTab[entityIndex]));
                        entityIndex += 2;
                        
                        temporalAggregationIntervalExtended = false;
                	}       	
                }
                if(!temporalAggregationIntervalExtended) {//save previous poi node and contextual nodes if the interval not extended. Otherwise do not save it because already saved and to prevent an error               	
                    previous_no2 = no2;
                    previous_pm10 = pm10;
                    previous_pm11 = pm11;
                    previous_pm12 = pm12;
                    previous_bc = bc;
                    previous_temp = temp;
                    previous_hum = hum;
                    previous_act = act;
                    previous_eve = eve;               
                    previous_fromNode = fromNode;   
                }                 
            }
            else {
                log.warn("print final poiFound");
                log.warn(poiFound.toString());
            }
        }
        log.warn("Finish or what??????????????????????????????????????????????????????????????????????");
        for (int i = 0; i < entityIndex; ++i) {
            if (i % 2 != 0) {
                RelationshipType relt = ((Relationship)entityTab[i]).getType();
                log.warn("Relationship:");
                log.warn(relt.name());
            }
            else {
                Iterable<Label> l = (Iterable<Label>)((Node)entityTab[i]).getLabels();
                log.warn("Node:");
                for (Label li : l) {
                    log.warn(li.name());
                }
            }
            log.warn(Integer.toString(i));
        }
    	
    	GraphResult graphResult = new GraphResult(servers, relationships);
    	createGraph(relationships); //relationships contains starts and end nodes also
    	
        return Stream.of(graphResult); 	
    }
  
    /*
     * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
     * Below for context based semantic trajectories
     * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   	
    */
    
    @Procedure(mode = Mode.WRITE, value = "example.getContextualBasedTrajGraph")
    public Stream<GraphResult> getContextualBasedTrajGraph(@Name("participantId") Long participantId, @Name("ContextInterests") String contDimension, @Name("start time") String startTime, @Name("end time")  String endTime) {
        String query = "MATCH (timeInterval:TimeInterval {participantID : toString(" + participantId + ") })-[:HAS_PLACE]->(p:Place) ";
        if (startTime != null && startTime != "") {
            query = String.valueOf(query) + " WHERE timeInterval.start > toString('" + startTime + "')";
            
            if (endTime != null && endTime != "" ) {
                query = String.valueOf(query) + " AND timeInterval.end < toString('" + endTime + "')";
            }
        }
        else if (endTime != null && endTime != "" ) { // if there is only endtime
        	query = String.valueOf(query) + " WHERE timeInterval.end < toString('" + endTime + "')";
        }
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_NO2_SEMANTIC]->(no2:No2Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_PM10_SEMANTIC]->(pm10:Pm10Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_PM1_0_SEMANTIC]->(pm1:Pm1_0Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_PM2_5_SEMANTIC]->(pm2:Pm2_5Semantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_BC_SEMANTIC]->(bc:BCSemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_TEMPERATURE_SEMANTIC]->(temp:TemperatureSemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_HUMIDITY_SEMANTIC]->(hum:HumiditySemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_ACTIVITY_SEMANTIC]->(act:ActivitySemantic)";
        query = String.valueOf(query) + " MATCH (timeInterval)-[:HAS_EVENT_SEMANTIC]->(eve:EventSemantic)";
        query = String.valueOf(query) + " RETURN * ORDER BY timeInterval.start";
        
        Result result = this.tx.execute(query);
        
        Entity[] entityTab = new Entity[100000];
        
        List<Node> servers = new LinkedList<Node>();
        List<Relationship> relationships = new LinkedList<Relationship>();
        
        int entityIndex = 0;
        
        VirtualNode fromNode = null;
        
        Boolean temporalAggregationIntervalExtended = false;
        Node previous_place = null;
        Node previous_no2 = null;
        Node previous_pm10 = null;
        Node previous_pm11 = null;
        Node previous_pm12 = null;
        Node previous_bc = null;
        Node previous_temp = null;
        Node previous_hum = null;
        Node previous_act = null;
        Node previous_eve = null;
        
        VirtualNode previous_fromNode = null;
        
        while (result.hasNext()) {
            Map<String, Object> row = (Map<String, Object>)result.next();
            Node timeInterval = (Node) row.get("timeInterval");
            Node place = (Node) row.get("p");
            Node no2 = (Node) row.get("no2");
            Node pm10 = (Node) row.get("pm10");
            Node pm11 = (Node) row.get("pm1");
            Node pm12 = (Node) row.get("pm2");
            Node bc = (Node) row.get("bc");
            Node temp = (Node) row.get("temp");
            Node hum = (Node) row.get("hum");
            Node act = (Node) row.get("act");
            Node eve = (Node) row.get("eve");
            RelationshipType no2Relashionship = RelationshipType.withName("HAS_NO2_SEMANTIC");
            RelationshipType pm10Relashionship = RelationshipType.withName("HAS_PM10_SEMANTIC");
            RelationshipType pm1Relashionship = RelationshipType.withName("HAS_PM1_0_SEMANTIC");
            RelationshipType pm2tRelashionship = RelationshipType.withName("HAS_PM2_5_SEMANTIC");
            RelationshipType bcRelashionship = RelationshipType.withName("HAS_BC_SEMANTIC");
            RelationshipType tempRelashionship = RelationshipType.withName("HAS_TEMPERATURE_SEMANTIC");
            RelationshipType humRelashionship = RelationshipType.withName("HAS_HUMIDITY_SEMANTIC");
            RelationshipType actRelashionship = RelationshipType.withName("HAS_ACTIVITY_SEMANTIC");
            RelationshipType eveRelashionship = RelationshipType.withName("HAS_EVENT_SEMANTIC");
            RelationshipType palceRelashionship = RelationshipType.withName("HAS_PLACE");
            
                      
            RelationshipType nextRelashionship = RelationshipType.withName("NEXT");
            
            List<String> labelPOIs = new ArrayList<String>();
            labelPOIs.add("POI");
            Label[] poiLabels = Util.labels((Object)labelPOIs);
                   
            switch(contDimension) {
      	  case "No2Semantic":      
      		  fromNode = new VirtualNode(poiLabels, no2.getAllProperties());
    		  fromNode.setProperty("contextOfInterest", contDimension);                	                  
      		  break;
      		   
      	  case "Pm10Semantic":
      		fromNode = new VirtualNode(poiLabels, pm10.getAllProperties());
    		  fromNode.setProperty("contextOfInterest", contDimension);
      		  break;
      		   
      	  case "Pm1_0Semantic":
      		fromNode = new VirtualNode(poiLabels, pm11.getAllProperties());
    		  fromNode.setProperty("contextOfInterest", contDimension);
      		  break;
      		  
      	  case "Pm2_5Semantic":
      		fromNode = new VirtualNode(poiLabels, pm12.getAllProperties());
    		  fromNode.setProperty("contextOfInterest", contDimension);
      		  break;
      		   
      	  case "BCSemantic":
      		fromNode = new VirtualNode(poiLabels, bc.getAllProperties());
    		  fromNode.setProperty("contextOfInterest", contDimension);
      		  break;
      		   
      	  case "TemperatureSemantic":
      		fromNode = new VirtualNode(poiLabels, temp.getAllProperties());
    		  fromNode.setProperty("contextOfInterest", contDimension);
      		  break;
      		   
      	  case "HumiditySemantic":
      		  fromNode = new VirtualNode(poiLabels, hum.getAllProperties());
    		  fromNode.setProperty("contextOfInterest", contDimension);
    		  break;
      		   
      	  case "ActivitySemantic":
      		  fromNode = new VirtualNode(poiLabels, act.getAllProperties());
    		  fromNode.setProperty("contextOfInterest", contDimension);  		 
      		  break;
      		  
      	  case "EventSemantic":
      		  fromNode = new VirtualNode(poiLabels, eve.getAllProperties());
      		  fromNode.setProperty("contextOfInterest", contDimension);
      		  break;      

      	  default: break;
      	    
      	}
        
            for (String key : timeInterval.getPropertyKeys()) {
            fromNode.setProperty(key, timeInterval.getProperty(key));
            }

            if (entityIndex == 0) {
                entityTab[entityIndex] = (Entity)fromNode;
                ++entityIndex;
                servers.add((Node)fromNode);
                if(!contDimension.equals("No2Semantic")) {
                	servers.add(no2);
                	relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(no2, no2Relashionship));
                }
                if(!contDimension.equals("Pm10Semantic")) {
                	servers.add(pm10);
                	relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(pm10, pm10Relashionship));
                }
                if(!contDimension.equals("Pm1_0Semantic")) {
                	servers.add(pm11);
                	relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(pm11, pm1Relashionship));
                }                
                if(!contDimension.equals("Pm2_5Semantic")) {
                	servers.add(pm12);
                	relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(pm12, pm2tRelashionship));
                }
                if(!contDimension.equals("BCSemantic")) {
                	servers.add(bc);
                	relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(bc, bcRelashionship));
                }
                if(!contDimension.equals("TemperatureSemantic")) {
                	servers.add(temp);
                	relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(temp, tempRelashionship));
                }
                if(!contDimension.equals("HumiditySemantic")) {
                	servers.add(hum);
                	relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(hum, humRelashionship));
                }
                if(!contDimension.equals("ActivitySemantic")) {
                	servers.add(act);
                	relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(act, actRelashionship));
                }
                if(!contDimension.equals("EventSemantic")) {
                	servers.add(eve);
                	relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(eve, eveRelashionship));
                }
                servers.add(place);
            	relationships.add(((Node)entityTab[entityIndex - 1]).createRelationshipTo(place, palceRelashionship));              
            }
            else {
            	
            	if(previous_fromNode.getProperty("contextOfInterest").equals(fromNode.getProperty("contextOfInterest"))
            			&& previous_place.equals(place) && previous_no2.equals(no2) && previous_pm10.equals(pm10) && previous_pm11.equals(pm11) && previous_pm12.equals(pm12) && previous_bc.equals(bc)
            			&& previous_temp.equals(temp) && previous_hum.equals(hum) && previous_act.equals(act) && previous_eve.equals(eve)) {//if previous node contextual and spatial values equal actual node
            		
            		int index = servers.lastIndexOf((Node)previous_fromNode);
            		log.warn("INTERVAL EXTENDED");
            		log.warn(Integer.toString(index));
            		servers.get(index).setProperty("end", fromNode.getProperty("end")); //update end timeinterval for temporal aggregation of the segment
            		temporalAggregationIntervalExtended = true;                		        		
            	}
            	else {
            		entityTab[entityIndex + 1] = (Entity)fromNode;
                    servers.add((Node)(entityTab[entityIndex + 1]));
                    
                    if(!contDimension.equals("No2Semantic")) {
                    	servers.add(no2);
                    	 relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(no2, no2Relashionship));
                    }
                    if(!contDimension.equals("Pm10Semantic")) {
                    	servers.add(pm10);
                    	relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(pm10, pm10Relashionship));
                    }
                    if(!contDimension.equals("Pm1_0Semantic")) {
                    	servers.add(pm11);
                    	relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(pm11, pm1Relashionship));
                    }                
                    if(!contDimension.equals("Pm2_5Semantic")) {
                    	servers.add(pm12);
                    	relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(pm12, pm2tRelashionship));
                    }
                    if(!contDimension.equals("BCSemantic")) {
                    	servers.add(bc);
                    	relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(bc, bcRelashionship));
                    }
                    if(!contDimension.equals("TemperatureSemantic")) {
                    	servers.add(temp);
                    	relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(temp, tempRelashionship));
                    }
                    if(!contDimension.equals("HumiditySemantic")) {
                    	servers.add(hum);
                    	relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(hum, humRelashionship));
                    }
                    if(!contDimension.equals("ActivitySemantic")) {
                    	servers.add(act);
                    	relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(act, actRelashionship));
                    }
                    if(!contDimension.equals("EventSemantic")) {
                    	servers.add(eve);
                    	relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(eve, eveRelashionship));
                    }
                    servers.add(place);
                	relationships.add(((Node)entityTab[entityIndex + 1]).createRelationshipTo(place, palceRelashionship));   
                	
                    final VirtualRelationship vrel = (VirtualRelationship)((Node)entityTab[entityIndex - 1]).createRelationshipTo((Node)entityTab[entityIndex + 1], nextRelashionship);
                    entityTab[entityIndex] = (Entity)vrel;
                    relationships.add((Relationship)(entityTab[entityIndex]));
                    entityIndex += 2;
                    
                    temporalAggregationIntervalExtended = false;
            	}       	
            }
            if(!temporalAggregationIntervalExtended) {//save previous poi node and contextual nodes if the interval not extended. Otherwise do not save it because already saved and to prevent an error               	
                previous_place = place;
            	previous_no2 = no2;
                previous_pm10 = pm10;
                previous_pm11 = pm11;
                previous_pm12 = pm12;
                previous_bc = bc;
                previous_temp = temp;
                previous_hum = hum;
                previous_act = act;
                previous_eve = eve;               
                previous_fromNode = fromNode;   
            }                 
        }
        for (int i = 0; i < entityIndex; ++i) {
            if (i % 2 != 0) {
                RelationshipType relt = ((Relationship)entityTab[i]).getType();
                log.warn("Relationship:");
                log.warn(relt.name());
            }
            else {
                Iterable<Label> l = (Iterable<Label>)((Node)entityTab[i]).getLabels();
                log.warn("Node:");
                for (Label li : l) {
                    log.warn(li.name());
                }
            }
            log.warn(Integer.toString(i));
        }
        
        GraphResult graphResult = new GraphResult(servers, relationships);
        createGraph(relationships); //relationships contains starts and end nodes also
        
        return Stream.of(graphResult);
    }
    

    /*
     * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
     * Below Utils
     * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!   	
    */
    
    public void createGraph(List<Relationship> relationships) {

    	Node startNode = null;
    	List<String> startNodeLabelNames = new LinkedList<String>();
		Map<String, Object> startNodeProps = null;
		Node endNode = null;
		List<String> endNodeLabelNames = new LinkedList<String>();
		Map<String, Object> endNodeProps = null;
    	String relType = null;

    	Map<String, Object> rProps = null;
    	
    	for (Relationship r : relationships) { 	

    		startNode = r.getStartNode();
    		endNode = r.getEndNode();
    		
    		for (Label label : startNode.getLabels()) {
    			startNodeLabelNames.add(label.name());
    			}
    		startNodeProps = startNode.getAllProperties();   
    		String startNodelabels = Util.labelString(startNodeLabelNames);
            String startNodeAllProps = buildPropsStringForCypher(startNodeProps);
    		
            for (Label label : endNode.getLabels()) {
    			endNodeLabelNames.add(label.name());
    			}
    		endNodeProps = endNode.getAllProperties();   
    		String endNodelabels = Util.labelString(endNodeLabelNames);
            String endNodeAllProps = buildPropsStringForCypher(endNodeProps);   		
 	
    		relType = r.getType().name();    		
    		
    		rProps = r.getAllProperties();
    		String rAllProps = buildPropsStringForCypher(rProps);
    		    		
    		//Map<String, Object> params = Util.map("ridentProps", rProps, "startidentProps", startNodeProps, "endidentProps", endNodeProps);
    		    		
    		final String cypher = "MERGE(startn:" + startNodelabels + "{" + startNodeAllProps + "}) " +
    					   		  "MERGE(endn:" + endNodelabels + "{" + endNodeAllProps + "}) " +
    					   		  "MERGE(startn)-[r:"+ Util.quote(relType) +"{"+rAllProps+"}]->(endn) " ;
            
            tx.execute(cypher);
            		
            startNodeLabelNames.clear();
            endNodeLabelNames.clear();
    	}
    }
    
    
    public void createGraphNotWorking(List<Node> nodes, List<Relationship> relationships) {
		List<String> labelNames = new LinkedList<String>();
		//Map<String, Object> identProps = null;
		//Map<String, Object> onMatchProps = null;
		Map<String, Object> props = null;
		
		List<Node> listCreatedNodes = new LinkedList<Node>();
		
    	for (Node n : nodes) { 		    		
    		try {
        		for (Label label : n.getLabels()) {
        			labelNames.add(label.name());          	
        			
        		/*	if(label.name().equals("No2Semantic") || label.name().equals("Pm10Semantic") || label.name().equals("Pm1_0Semantic") || label.name().equals("Pm2_5Semantic") 
        					|| label.name().equals("BCSemantic") || label.name().equals("TemperatureSemantic") || label.name().equals("HumiditySemantic")) {
        				
        				identProps = new HashMap<String, Object>() {{ put("level", n.getProperty("level")); }};
        				
        				//identProps.put("level", n.getProperty("level"));
        			}
        			else if(label.name().equals("IndoorPlace")) {
        				identProps = new HashMap<String, Object>() {{ put("name", n.getProperty("partition_name")); }};
        				
        				//identProps.put("name", n.getProperty("partition_name"));
        			}
        			else {

        				identProps = new HashMap<String, Object>() {{ put("name", n.getProperty("name")); }};
        				
        				//identProps.put("name", n.getProperty("name"));
        			}
        			*/
        		}   
        		
        		props = n.getAllProperties();   
        		
        		
        		/*if (identProps==null || identProps.isEmpty()) {
                    throw new IllegalArgumentException("you need to supply at least one identifying property for a merge");
                }*/
               
                String labels = Util.labelString(labelNames);
                
                //Map<String, Object> params = Util.map("identProps", identProps, "onCreateProps", props, "onMatchProps", onMatchProps);
                //String identPropsString = buildIdentPropsString(identProps);
                Map<String, Object> params = Util.map("identProps", props);
                String allProps = buildIdentPropsString(props);
                
                
                final String cypher = "MERGE (n:" + labels + "{" + allProps + "})"; // ON CREATE SET n += $onCreateProps ON MATCH SET n += $onMatchProps RETURN n";
                Stream<NodeResult> createdNode= tx.execute(cypher, params ).columnAs("n").stream().map(node -> new NodeResult((Node) node));    
                long l = createdNode.count();
                log.warn(Long.toString(l));
                
                listCreatedNodes.add(createdNode.findFirst().get().node); //save created nodes in order to be able to create the relationships between them
               
        		labelNames.clear();
        		//identProps = Collections.emptyMap();
        		
        		}catch(Exception e) {
        			StackTraceElement[] stktrace = e.getStackTrace();
        			// print element of stktrace
        			for (int x = 0; x < stktrace.length; x++) {
	                 log.warn("Index " + x
                             + " of stack trace"
                             + " array conatins = "
                             + stktrace[x].toString());
	             }
        			throw new IllegalArgumentException("_____________________________________________________________________________"); 			
    		}
    		
    	} log.warn("FINISH CREATING NODES.....");
    	
    	Node startNode = null;
    	String relType = null;
    	Node endNode = null;
    	//identProps.clear();
    	Map<String, Object> rProps = null;
    	//Map<String, Object> onCreateProps = null;
    	//onMatchProps = null;
    	
    	int j=0;
    	
    	for (Relationship r : relationships) { 	

    		log.warn("CREATING A Relationship.....");
    		
    		startNode = listCreatedNodes.stream().filter(n -> n.getAllProperties().equals(r.getStartNode().getAllProperties())).findFirst().get();
    		endNode = listCreatedNodes.stream().filter(n -> n.getAllProperties().equals(r.getEndNode().getAllProperties())).findFirst().get();
    		//startNode = r.getStartNode();
    		//endNode = r.getEndNode();
    		
    		for (Map.Entry<String, Object> entry : startNode.getAllProperties().entrySet()) {
    			log.warn(entry.getKey());
    			log.warn((String)entry.getValue());
    		}
    		
    		relType = r.getType().name();
    		
    		rProps = r.getAllProperties();
    		String rAllProps = buildIdentPropsString(rProps);
    		
    		/*
            Map<String, Object> params = Util.map("identProps", identProps, "onCreateProps", onCreateProps==null ? emptyMap() : onCreateProps,
                    "onMatchProps", onMatchProps == null ? emptyMap() : onMatchProps, "startNode", startNode, "endNode", endNode);
                     
            final String cypher =
                    "WITH $startNode as startNode, $endNode as endNode " +
                    "MERGE (startNode)-[r:"+ Util.quote(relType) +"{"+identPropsString+"}]->(endNode) " +
                    "ON CREATE SET r+= $onCreateProps " +
                    "ON MATCH SET r+= $onMatchProps " +
                    "RETURN r"; 
            */
            
            Map<String, Object> params = Util.map("identProps", rProps, "startNode", startNode, "endNode", endNode);
            
            final String cypher =
                    "WITH $startNode as startNode, $endNode as endNode " +
                    "MERGE (startNode)-[r:"+ Util.quote(relType) +"{"+rAllProps+"}]->(endNode) " +
                    "RETURN r";
            log.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            log.warn(cypher);
            log.warn("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            tx.execute(cypher, params ).columnAs("r").stream().map(rel -> new RelationshipResult((Relationship) rel));
            
            log.warn(Integer.toString(j));
    		j++;
    	} log.warn("FINISH CREATING RELATIONSHIPS.....");
    }
    
    private String buildPropsStringForCypher(Map<String, Object> props) {
        if (props == null) return "";
        
        return props.entrySet().stream()
        		.map(entry -> entry.getKey() + ": toString('" + entry.getValue().toString().replace("'", "\\'") + "') ")
        		.collect(Collectors.joining(", "));
        	
    }
    
    private String buildIdentPropsString(Map<String, Object> identProps) {
        if (identProps == null) return "";
        return identProps.keySet().stream().map(Util::quote)
                .map(s -> s + ":$identProps." + s)
                .collect(Collectors.joining(","));
    }
    
}
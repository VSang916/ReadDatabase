// package com.sang.ThuVien.controller;

// import java.util.ArrayList;
// import java.util.HashMap;
// import java.util.List;
// import java.util.Map;

// import org.bson.Document;
// import org.springframework.http.HttpStatus;
// import org.springframework.http.ResponseEntity;
// import org.springframework.web.bind.annotation.GetMapping;
// import org.springframework.web.bind.annotation.PathVariable;
// import org.springframework.web.bind.annotation.PostMapping;
// import org.springframework.web.bind.annotation.RequestBody;
// import org.springframework.web.bind.annotation.RequestMapping;
// import org.springframework.web.bind.annotation.RequestParam;
// import org.springframework.web.bind.annotation.RestController;

// import com.mongodb.ConnectionString;
// import com.mongodb.MongoClientSettings;
// import com.mongodb.ServerApi;
// import com.mongodb.ServerApiVersion;
// import com.mongodb.client.FindIterable;
// import com.mongodb.client.MongoClient;
// import com.mongodb.client.MongoClients;
// import com.mongodb.client.MongoCollection;
// import com.mongodb.client.MongoCursor;
// import com.mongodb.client.MongoDatabase;
// import com.mongodb.client.MongoIterable;

// @RestController
// @RequestMapping("/api/mongodb")
// public class MongoDbController {

//     MongoClient mongoConnection(String db) {
//         String url = "mongodb://localhost:27017/" + db;
//         String user = "mongo";
//         String password = "mongo";
        
//         try {
//             String connectionString = String.format("mongodb://%s:%s@localhost:27017/%s?authSource=admin", 
//                 user, password, db);

//             ServerApi serverApi = ServerApi.builder()
//                 .version(ServerApiVersion.V1)
//                 .build();

//             MongoClientSettings settings = MongoClientSettings.builder()
//                 .applyConnectionString(new ConnectionString(connectionString))
//                 .serverApi(serverApi)
//                 .build();

//             return MongoClients.create(settings);
//         } catch (Exception e) {
//             System.out.println("MongoDB Connection Error: " + e.getMessage());
//         }
//         return null;
//     }

//     @GetMapping("/databases")
//     public List<String> getDatabases() {
//         List<String> databases = new ArrayList<>();
//         try (MongoClient mongoClient = mongoConnection("test")) {
//             if (mongoClient != null) {
//                 MongoIterable<String> dbNames = mongoClient.listDatabaseNames();
//                 for (String dbName : dbNames) {
//                     databases.add(dbName);
//                 }
//             }
//         }
//         return databases;
//     }

//     @GetMapping("/{database}/collections")
//     public List<Map<String, Object>> getCollections(@PathVariable String database) {
//         List<Map<String, Object>> collections = new ArrayList<>();
//         try (MongoClient mongoClient = mongoConnection(database)) {
//             if (mongoClient != null) {
//                 MongoDatabase db = mongoClient.getDatabase(database);
//                 for (String collectionName : db.listCollectionNames()) {
//                     Map<String, Object> collection = new HashMap<>();
//                     collection.put("database", database);
//                     collection.put("collection_name", collectionName);
//                     collections.add(collection);
//                 }
//             }
//         }
//         return collections;
//     }

//     @GetMapping("/collections/structure")
//     public List<Map<String, Object>> getCollectionStructure(
//             @RequestParam String database,
//             @RequestParam String collection) {
//         List<Map<String, Object>> structure = new ArrayList<>();
        
//         try (MongoClient mongoClient = mongoConnection(database)) {
//             if (mongoClient != null) {
//                 MongoDatabase db = mongoClient.getDatabase(database);
//                 MongoCollection<Document> coll = db.getCollection(collection);
                
//                 // Lấy một document mẫu để phân tích cấu trúc
//                 FindIterable<Document> sampleDoc = coll.find();
//                 for ( Document ex : sampleDoc){
//                 if (ex != null) {
//                     Map<String, Object> collectionInfo = new HashMap<>();
//                     collectionInfo.put("database", database);
//                     collectionInfo.put("collection", collection);
                    
//                     List<Map<String, Object>> fields = new ArrayList<>();
//                     analyzeDocument(ex, "", fields);
                    
//                     collectionInfo.put("fields", fields);
//                     structure.add(collectionInfo);
//                 }
//             }
//             }
//         }
//         return structure;
//     }

//     private void analyzeDocument(Document doc, String prefix, List<Map<String, Object>> fields) {
//         for (Map.Entry<String, Object> entry : doc.entrySet()) {
//             String fieldName = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
//             Object value = entry.getValue();
            
//             Map<String, Object> field = new HashMap<>();
//             field.put("field_name", fieldName);
            
//             if (value instanceof Document) {
//                 field.put("data_type", "Object");
//                 fields.add(field);
//                 analyzeDocument((Document) value, fieldName, fields);
//             } else {
//                 field.put("data_type", value != null ? value.getClass().getSimpleName() : "null");
//                 fields.add(field);
//             }
//         }
//     }

//     @PostMapping("/query")
//     public ResponseEntity<Map<String, Object>> executeMongoQuery(@RequestBody Map<String, String> request) {
//         Map<String, Object> response = new HashMap<>();
//         List<Map<String, Object>> results = new ArrayList<>();

//         String query = request.get("query");
//         String database = request.get("database");
//         String collection = request.get("collection");
//         String operation = request.get("operation");

//         if (query == null || query.trim().isEmpty()) {
//             response.put("success", false);
//             response.put("message", "MongoDB query is required.");
//             return ResponseEntity.badRequest().body(response);
//         }

//         try (MongoClient mongoClient = mongoConnection(database)) {
//             if (mongoClient != null) {
//                 MongoDatabase db = mongoClient.getDatabase(database);
//                 MongoCollection<Document> coll = db.getCollection(collection);
                
//                 Document queryDoc = Document.parse(query);
                
//                 switch (operation.toUpperCase()) {
//                     case "FIND":
//                         try (MongoCursor<Document> cursor = coll.find(queryDoc).iterator()) {
//                             while (cursor.hasNext()) {
//                                 Document doc = cursor.next();
//                                 results.add(convertDocumentToMap(doc));
//                             }
//                             response.put("success", true);
//                             if (results.isEmpty()) {
//                                 response.put("message", "Query executed successfully, but no data found.");
//                             } else {
//                                 response.put("data", results);
//                             }
//                         }
//                         break;

//                     case "INSERT":
//                         coll.insertOne(queryDoc);
//                         response.put("success", true);
//                         response.put("message", "Document inserted successfully");
//                         break;

//                     case "UPDATE":
//                         String filter = request.get("filter");
//                         Document filterDoc = Document.parse(filter);
//                         Document updateDoc = new Document("$set", queryDoc);
//                         long modifiedCount = coll.updateMany(filterDoc, updateDoc).getModifiedCount();
//                         response.put("success", true);
//                         response.put("message", modifiedCount + " document(s) updated");
//                         break;

//                     case "DELETE":
//                         long deletedCount = coll.deleteMany(queryDoc).getDeletedCount();
//                         response.put("success", true);
//                         response.put("message", deletedCount + " document(s) deleted");
//                         break;

//                     default:
//                         response.put("success", false);
//                         response.put("message", "Unsupported operation");
//                         return ResponseEntity.badRequest().body(response);
//                 }
//             } else {
//                 response.put("success", false);
//                 response.put("message", "Could not connect to MongoDB");
//                 return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
//             }
//         } catch (Exception e) {
//             response.put("success", false);
//             response.put("message", "Error executing MongoDB query: " + e.getMessage());
//             return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
//         }

//         return ResponseEntity.ok(response);
//     }

//     private Map<String, Object> convertDocumentToMap(Document doc) {
//         Map<String, Object> map = new HashMap<>();
//         for (Map.Entry<String, Object> entry : doc.entrySet()) {
//             String key = entry.getKey();
//             Object value = entry.getValue();
            
//             if (value instanceof Document) {
//                 map.put(key, convertDocumentToMap((Document) value));
//             } else {
//                 map.put(key, value);
//             }
//         }
//         return map;
//     }
// }
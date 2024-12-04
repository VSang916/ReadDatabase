// package com.sang.ThuVien.controller;

// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.data.mongodb.core.MongoTemplate;
// import org.springframework.data.mongodb.core.query.Criteria;
// import org.springframework.data.mongodb.core.query.Query;
// import org.springframework.data.redis.core.RedisTemplate;
// import org.springframework.data.redis.core.ValueOperations;
// import org.springframework.web.bind.annotation.*;
// import org.elasticsearch.action.search.SearchRequest;
// import org.elasticsearch.action.search.SearchResponse;
// import org.elasticsearch.client.RequestOptions;
// import org.elasticsearch.client.RestHighLevelClient;
// import org.elasticsearch.index.query.BoolQueryBuilder;
// import org.elasticsearch.index.query.QueryBuilders;
// import org.elasticsearch.search.builder.SearchSourceBuilder;

// import java.io.IOException;
// import java.util.*;

// @RestController
// @RequestMapping("/api/nosql")
// public class NoSQLController {

//     @Autowired
//     private MongoTemplate mongoTemplate;

//     @Autowired
//     private RestHighLevelClient elasticsearchClient;

//     @Autowired
//     private RedisTemplate<String, Object> redisTemplate;

//     // MongoDB: Tìm tài liệu theo một số trường nhất định
//     @GetMapping("/mongodb/search")
//     public List<Map> searchInMongoDB(@RequestParam Map<String, Object> queryParameters) {
//         Query query = new Query();
//         queryParameters.forEach((key, value) -> query.addCriteria(Criteria.where(key).is(value)));

//         return mongoTemplate.find(query, Map.class, "collectionName");
//     }

//     // Elasticsearch: Tìm kiếm theo nhiều trường
//     @GetMapping("/elasticsearch/search")
//     public List<Map<String, Object>> searchInElasticsearch(@RequestParam Map<String, String> queryParameters) throws IOException {
//         SearchRequest searchRequest = new SearchRequest("indexName"); // Tên chỉ mục của bạn
//         BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

//         // Xây dựng truy vấn Bool Query cho các trường
//         queryParameters.forEach((key, value) -> boolQuery.must(QueryBuilders.matchQuery(key, value)));

//         SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//         searchSourceBuilder.query(boolQuery);
//         searchRequest.source(searchSourceBuilder);

//         SearchResponse searchResponse = elasticsearchClient.search(searchRequest, RequestOptions.DEFAULT);

//         // Chuyển đổi kết quả thành danh sách Map
//         List<Map<String, Object>> results = new ArrayList<>();
//         searchResponse.getHits().forEach(hit -> results.add(hit.getSourceAsMap()));

//         return results;
//     }

//     // Redis: Lấy và lưu trữ dữ liệu với Redis
//     @GetMapping("/redis/get")
//     public Object getFromRedis(@RequestParam String key) {
//         ValueOperations<String, Object> valueOps = redisTemplate.opsForValue();
//         return valueOps.get(key);
//     }

//     @PostMapping("/redis/set")
//     public String setInRedis(@RequestParam String key, @RequestBody Object value) {
//         ValueOperations<String, Object> valueOps = redisTemplate.opsForValue();
//         valueOps.set(key, value);
//         return "Đã lưu dữ liệu vào Redis với khóa: " + key;
//     }
// }

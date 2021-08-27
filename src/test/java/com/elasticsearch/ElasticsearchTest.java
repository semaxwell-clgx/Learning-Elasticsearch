package com.elasticsearch;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class ElasticsearchTest {

    @Autowired
    RestHighLevelClient client;


    /*
     * 3.a
     */
    @Test
    public void givenRequestForAllDocuments_whenRequestIsExecuted_10DocumentsAreReturned() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        SearchRequest searchRequest = new SearchRequest("courses");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(10l, response.getHits().getTotalHits().value);
    }

    /*
     * 3.b
     *  Results do not include course with name 'Theatre 410'
     */
    @Test
    public void given10CoursesAnd9HaveProfessorsWithEmails_whenQueryingForCoursesWhoseProfessorsHaveEmails_9AreReturned() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.existsQuery("professor.email"));

        SearchRequest searchRequest = new SearchRequest("courses");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(9l, response.getHits().getTotalHits().value);
    }

    /*
     * 3.c
     */
    @Test
    public void given10Courses_whenSearchingForCoursesWithNameContainingTheWordComputer_2CoursesAreReturned() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("name", "computer"));

        SearchRequest searchRequest = new SearchRequest("courses");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(2l, response.getHits().getTotalHits().value);
    }

    /*
     * 3.d
     */
    @Test
    public void given10Courses_whenSearchingForCoursesWithNameContainingComputerAndRoomEqualToc8_then1CourseIsReturned() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("name", "computer"))
                .must(QueryBuilders.matchQuery("room", "c8"))
        );

        SearchRequest searchRequest = new SearchRequest("courses");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(1l, response.getHits().getTotalHits().value);
    }

    /*
     * 3.e
     */
    @Test
    public void given10courses_whenNameIsAccountingAndRoomIse3ORwhenNameIsComputerANDprofessorNameIsGregg_thenReturn2Courses() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.boolQuery()
                .should(QueryBuilders.matchQuery("name", "accounting"))
                .should(QueryBuilders.matchQuery("room", "e3"))
                .should(QueryBuilders.matchQuery("name", "computer"))
                .should(QueryBuilders.matchQuery("professor.name", "gregg"))
                .minimumShouldMatch(2)
        );

        SearchRequest searchRequest = new SearchRequest("courses");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(4l, response.getHits().getTotalHits().value);
    }

    /*
     * 3.f
     */
    @Test
    public void given10courses_whenQueryingForEitherNameOrProfessorDepartmentToContainAccounting_4CoursesAreReturned() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.multiMatchQuery("accounting", "name", "professor.department"));

        SearchRequest searchRequest = new SearchRequest("courses");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(4l, response.getHits().getTotalHits().value);
    }

    /*
     * 3.g
     */
    @Test
    public void give10courses_whenSearchingForCoursesContainingAPhrase_1MatchIsReturned() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchPhraseQuery("course_description", "from the business school taken by"));

        SearchRequest searchRequest = new SearchRequest("courses");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(1l, response.getHits().getTotalHits().value);
    }

    /*
     * 3.h
     */
    @Test
    public void give10courses_whenPrefixSearchingForCoursesContainingAPhrase_1PartialMatchIsReturned() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchPhrasePrefixQuery("course_description", "from the business school taken by fin"));

        SearchRequest searchRequest = new SearchRequest("courses");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(1l, response.getHits().getTotalHits().value);
    }

    /*
     * 3.i
     */
    @Test
    public void give10courses_whenQueryingForCoursesWithMoreThan10ButLessThan30StudentsInclusive_then6AreReturned() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.rangeQuery("students_enrolled").gte(10).lte(30));

        SearchRequest searchRequest = new SearchRequest("courses");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(6l, response.getHits().getTotalHits().value);
    }

    /*
     * 3.j
     */
    @Test
    public void complicatedQuery() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("name", "accounting"))
                .mustNot(QueryBuilders.matchQuery("room", "e7"))
                .should(QueryBuilders.rangeQuery("students_enrolled").gte(10).lte(20))
                .minimumShouldMatch(1)
        );

        SearchRequest searchRequest = new SearchRequest("courses");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(1l, response.getHits().getTotalHits().value);
    }

    /*
     * 4.a
     */
    @Test
    public void given10Courses_whenFilteringByProfessorNameAndClassNameAndSearchForRoome3_shouldReturn1Course() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.boolQuery().filter(
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("professor.name", "bill"))
                        .must(QueryBuilders.matchQuery("name", "accounting")))
                .must(QueryBuilders.matchQuery("room", "e3"))
        );

        SearchRequest searchRequest = new SearchRequest("courses");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(1l, response.getHits().getTotalHits().value);
    }

    /*
     * 5.a
     */
    @Test
    public void givenNestedStructure_whenSearchingForAliceWhite_then1UserIsReturned() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.nestedQuery(
                "user",
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery("user.first", "Alice"))
                        .must(QueryBuilders.matchQuery("user.last", "White")),
                ScoreMode.None
        ));

        SearchRequest searchRequest = new SearchRequest("my-users");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(1l, response.getHits().getTotalHits().value);
    }

    /*
     * 5.b
     */
    @Test
    public void performNestedNestedQuery() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.nestedQuery(
                "driver",
                QueryBuilders.nestedQuery(
                        "driver.vehicle",
                        QueryBuilders.boolQuery()
                                .must(QueryBuilders.matchQuery("driver.vehicle.make", "Powell Motors"))
                                .must(QueryBuilders.matchQuery("driver.vehicle.model", "Canyonero")),
                        ScoreMode.None
                ),
                ScoreMode.None
        ));

        SearchRequest searchRequest = new SearchRequest("drivers");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(1l, response.getHits().getTotalHits().value);
    }
}
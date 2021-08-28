package com.elasticsearch;

import com.elasticsearch.domain.courses.Course;
import com.elasticsearch.domain.driver.DriverContainer;
import com.elasticsearch.domain.groups.Group;
import org.apache.lucene.search.join.ScoreMode;
import org.assertj.core.api.SoftAssertions;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.assertj.core.api.Assertions.*;

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

        List<Course> courses = TestUtils.extractCourses(response);
        assertThat(courses).hasSize(10);
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

        List<Course> courses = TestUtils.extractCourses(response);
        assertThat(courses)
                .isNotNull()
                .hasSize(9)
                .extracting(Course::getName)
                .doesNotContain("Theatre 410");
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

        List<Course> courses = TestUtils.extractCourses(response);
        assertThat(courses)
                .isNotNull()
                .hasSize(2)
                .extracting(Course::getName)
                .containsExactlyInAnyOrder("Computer Science 101", "Computer Internals 250");
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

        List<Course> courses = TestUtils.extractCourses(response);

        assertThat(courses)
                .as("Verify is not null")
                .isNotNull()
                .as("Verify size is 1")
                .hasSize(1)
                .extracting(Course::getName)
                .as("Verify class name is 'Computer Internals 250'")
                .contains("Computer Internals 250");
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

        List<Course> courses = TestUtils.extractCourses(response);
        SoftAssertions softly = new SoftAssertions(); // try out soft assertions
        softly.assertThat(courses)
                .isNotNull()
                .hasSize(4)
                .extracting("name", "room", "professor.name")
                .contains(
                        tuple("Computer Science 101", "C12", "Gregg Payne"),
                        tuple("Computer Internals 250", "C8", "Gregg Payne"),
                        tuple("Accounting 101", "E3", "Thomas Baszo"),
                        tuple("Accounting Info Systems 350", "E3", "Bill Cage"));
        softly.assertAll();
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

        List<Course> courses = TestUtils.extractCourses(response);
        assertThat(courses)
                .isNotNull()
                .hasSize(4)
                .extracting(Course::getName)
                .containsExactlyInAnyOrder(
                        "Cost Accounting 400",
                        "Accounting Info Systems 350",
                        "Accounting 101",
                        "Tax Accounting 200"
                );
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

        List<Course> courses = TestUtils.extractCourses(response);
        assertThat(courses)
                .isNotNull()
                .hasSize(1)
                .extracting("name", "room", "professor.name")
                .contains(tuple("Cost Accounting 400", "E7", "Bill Cage"));
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

        List<Course> courses = TestUtils.extractCourses(response);
        assertThat(courses)
                .isNotNull()
                .hasSize(1)
                .extracting("name", "room", "professor.name")
                .contains(tuple("Cost Accounting 400", "E7", "Bill Cage"));
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

        List<Course> courses = TestUtils.extractCourses(response);
        assertThat(courses)
                .isNotNull()
                .hasSize(6)
                .extracting(Course::getStudentsEnrolled)
                .contains(27l, 18l, 22l, 19l, 17l, 13l)
                .doesNotContain(33l, 47l, 31l, 33l);
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

        List<Course> courses = TestUtils.extractCourses(response);
        assertThat(courses)
                .isNotNull()
                .hasSize(1)
                .extracting("name", "room", "professor.name")
                .contains(tuple("Accounting Info Systems 350", "E3", "Bill Cage"));
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

        List<Course> courses = TestUtils.extractCourses(response);

        assertThat(courses)
                .isNotNull()
                .hasSize(1)
                .extracting("name", "room", "professor.name")
                .contains(tuple("Accounting Info Systems 350", "E3", "Bill Cage"));
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

        List<Group> groups = TestUtils.extractGroups(response);
        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(groups)
                .isNotNull()
                .hasSize(1)
                .extracting(Group::getGroup)
                .contains("fans");
        softly.assertAll();
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

        List<DriverContainer> drivers = TestUtils.extractDrivers(response);
        SoftAssertions softly = new SoftAssertions();
        softly.assertThat(drivers)
                .isNotNull()
                .hasSize(1)
                .extracting("driver.lastName")
                .contains("McQueen");
        softly.assertAll();
    }

    /*
     * 6.a
     */
    @Test
    public void given16Vehicles_whenPaginatedRequestIsMade_5VehiclesAreReturned() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery())
                .from(0)
                .size(5)
                .sort(SortBuilders.fieldSort("price").order(SortOrder.DESC));

        SearchRequest searchRequest = new SearchRequest("vehicles");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(5l, response.getHits().getHits().length);
    }

    /*
     * 6.b
     */
    @Test
    public void given16Vehicles_whenGettingCountOfToyotas_then2AreReturned() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("make", "toyota"))
                .size(0);

        SearchRequest searchRequest = new SearchRequest("vehicles");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEquals(2l, response.getHits().getTotalHits().value);
    }

    /*
     * 6.c
     */
    @Test
    public void given16Vehicles_whenGettingCountOfCarsByMake_then6BucketsAreReturned() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(
                AggregationBuilders.terms("popular_cars").field("make.keyword")
        );

        SearchRequest searchRequest = new SearchRequest("vehicles");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        var buckets = ((ParsedStringTerms) response.getAggregations().get("popular_cars")).getBuckets();

        assertEquals(6l, buckets.size());
    }

    /*
     * 6.d
     */
    @Test
    public void given16Vehicles_whenRequestingMinMaxAve_givenBucketsAreReturned() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(
                AggregationBuilders.terms("popular_cars").field("make.keyword")
                        .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
                        .subAggregation(AggregationBuilders.max("max_price").field("price"))
                        .subAggregation(AggregationBuilders.min("min_price").field("price"))
        );

        SearchRequest searchRequest = new SearchRequest("vehicles");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        var buckets = (((ParsedStringTerms) response.getAggregations().get("popular_cars")).getBuckets())
                .stream()
                .collect(Collectors.toMap(k->k.getKeyAsString(), k->k.getDocCount(), (k,v) -> k));

        assertAll("should be dodge=5,chevrolet=3,bmw=2,ford=2,honda=2,toyota=2",
                () -> assertEquals(6, buckets.size()),
                () -> assertEquals(true, buckets.containsKey("dodge")),
                () -> assertEquals(true, buckets.containsKey("chevrolet")),
                () -> assertEquals(true, buckets.containsKey("bmw")),
                () -> assertEquals(true, buckets.containsKey("ford")),
                () -> assertEquals(true, buckets.containsKey("honda")),
                () -> assertEquals(true, buckets.containsKey("toyota")),
                () -> assertEquals(5, buckets.get("dodge")),
                () -> assertEquals(3, buckets.get("chevrolet")),
                () -> assertEquals(2, buckets.get("bmw")),
                () -> assertEquals(2, buckets.get("ford")),
                () -> assertEquals(2, buckets.get("honda")),
                () -> assertEquals(2, buckets.get("toyota")));
    }

    /*
     * 6.e
     */
    @Test
    public void sameAs6dWithNoHitsAndRedCarsOnly() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder
                .query(QueryBuilders.matchQuery("color", "red"))
                .aggregation(
                        AggregationBuilders.terms("popular_cars").field("make.keyword")
                                .subAggregation(AggregationBuilders.avg("avg_price").field("price"))
                                .subAggregation(AggregationBuilders.max("max_price").field("price"))
                                .subAggregation(AggregationBuilders.min("min_price").field("price")) )
                .size(0);

        SearchRequest searchRequest = new SearchRequest("vehicles");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        var buckets = (((ParsedStringTerms) response.getAggregations().get("popular_cars")).getBuckets())
                .stream()
                .collect(Collectors.toMap(k->k.getKeyAsString(), k->k, (k,v) -> k));

        assertAll("should be dodge=3,chevrolet=1,bmw=1",
                () -> assertEquals(3, buckets.size()),

                () -> assertTrue(buckets.containsKey("dodge")),
                () -> assertEquals(3, buckets.get("dodge").getDocCount(), "Three dodges"),
                () -> assertEquals(35_000.0, ((ParsedMax) buckets.get("dodge").getAggregations().asMap().get("max_price")).getValue()),
                () -> assertEquals(24_000.0, ((ParsedAvg) buckets.get("dodge").getAggregations().asMap().get("avg_price")).getValue()),
                () -> assertEquals(18_000.0, ((ParsedMin) buckets.get("dodge").getAggregations().asMap().get("min_price")).getValue()),

                () -> assertTrue(buckets.containsKey("chevrolet")),
                () -> assertEquals(1, buckets.get("chevrolet").getDocCount(), "Three chevrolets"),
                () -> assertEquals(20_000.0, ((ParsedMax) buckets.get("chevrolet").getAggregations().asMap().get("max_price")).getValue()),
                () -> assertEquals(20_000.0, ((ParsedAvg) buckets.get("chevrolet").getAggregations().asMap().get("avg_price")).getValue()),
                () -> assertEquals(20_000.0, ((ParsedMin) buckets.get("chevrolet").getAggregations().asMap().get("min_price")).getValue()),

                () -> assertTrue(buckets.containsKey("bmw")),
                () -> assertEquals(1, buckets.get("bmw").getDocCount(), "Three bmws"),
                () -> assertEquals(80_000.0, ((ParsedMax) buckets.get("bmw").getAggregations().asMap().get("max_price")).getValue()),
                () -> assertEquals(80_000.0, ((ParsedAvg) buckets.get("bmw").getAggregations().asMap().get("avg_price")).getValue()),
                () -> assertEquals(80_000.0, ((ParsedMin) buckets.get("bmw").getAggregations().asMap().get("min_price")).getValue())
        );
    }

    /*
     * 6.c
     */
    @Test
    public void useStatsFunctionToGetMinMaxAvgSum() throws Exception {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(
                AggregationBuilders.terms("popular_cars").field("make.keyword")
                        .subAggregation(AggregationBuilders.stats("stats_on_price").field("price"))
        );

        SearchRequest searchRequest = new SearchRequest("vehicles");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        var buckets = (((ParsedStringTerms) response.getAggregations().get("popular_cars")).getBuckets())
                .stream()
                .collect(Collectors.toMap(k->k.getKeyAsString(), k->k, (k,v) -> k));

        assertAll("should be dodge=5,chevrolet=3,bmw=2,ford=2,honda=2,toyota=2",
                () -> assertEquals(6, buckets.size()),

                () -> assertTrue(buckets.containsKey("dodge")),
                () -> assertEquals(5, buckets.get("dodge").getDocCount(), "Five dodges"),
                () -> assertEquals(35_000.0, ((ParsedStats) buckets.get("dodge").getAggregations().asList().get(0)).getMax()),
                () -> assertEquals(18_900.0, ((ParsedStats) buckets.get("dodge").getAggregations().asList().get(0)).getAvg()),
                () -> assertEquals(10_000.0, ((ParsedStats) buckets.get("dodge").getAggregations().asList().get(0)).getMin()),
                () -> assertEquals(94_500.0, ((ParsedStats) buckets.get("dodge").getAggregations().asList().get(0)).getSum()),

                () -> assertTrue(buckets.containsKey("chevrolet")),
                () -> assertEquals(3, buckets.get("chevrolet").getDocCount(), "Five chevrolets"),
                () -> assertEquals(28_000.0, ((ParsedStats) buckets.get("chevrolet").getAggregations().asList().get(0)).getMax()),
                () -> assertEquals(20_333.333333333332, ((ParsedStats) buckets.get("chevrolet").getAggregations().asList().get(0)).getAvg()),
                () -> assertEquals(13_000.0, ((ParsedStats) buckets.get("chevrolet").getAggregations().asList().get(0)).getMin()),
                () -> assertEquals(61_000.0, ((ParsedStats) buckets.get("chevrolet").getAggregations().asList().get(0)).getSum()),

                () -> assertTrue(buckets.containsKey("bmw")),
                () -> assertEquals(2, buckets.get("bmw").getDocCount(), "Five bmws"),
                () -> assertEquals(80_000.0, ((ParsedStats) buckets.get("bmw").getAggregations().asList().get(0)).getMax()),
                () -> assertEquals(55_000.0, ((ParsedStats) buckets.get("bmw").getAggregations().asList().get(0)).getAvg()),
                () -> assertEquals(30_000.0, ((ParsedStats) buckets.get("bmw").getAggregations().asList().get(0)).getMin()),
                () -> assertEquals(110_000.0, ((ParsedStats) buckets.get("bmw").getAggregations().asList().get(0)).getSum()),

                () -> assertTrue(buckets.containsKey("ford")),
                () -> assertEquals(2, buckets.get("ford").getDocCount(), "Five ford"),
                () -> assertEquals(30_000.0, ((ParsedStats) buckets.get("ford").getAggregations().asList().get(0)).getMax()),
                () -> assertEquals(27_500.0, ((ParsedStats) buckets.get("ford").getAggregations().asList().get(0)).getAvg()),
                () -> assertEquals(25_000.0, ((ParsedStats) buckets.get("ford").getAggregations().asList().get(0)).getMin()),
                () -> assertEquals(55_000.0, ((ParsedStats) buckets.get("ford").getAggregations().asList().get(0)).getSum()),

                () -> assertTrue(buckets.containsKey("honda")),
                () -> assertEquals(2, buckets.get("honda").getDocCount(), "Five hondas"),
                () -> assertEquals(20_000.0, ((ParsedStats) buckets.get("honda").getAggregations().asList().get(0)).getMax()),
                () -> assertEquals(15_000.0, ((ParsedStats) buckets.get("honda").getAggregations().asList().get(0)).getAvg()),
                () -> assertEquals(10_000.0, ((ParsedStats) buckets.get("honda").getAggregations().asList().get(0)).getMin()),
                () -> assertEquals(30_000.0, ((ParsedStats) buckets.get("honda").getAggregations().asList().get(0)).getSum()),

                () -> assertTrue(buckets.containsKey("toyota")),
                () -> assertEquals(2, buckets.get("toyota").getDocCount(), "Five toyotas"),
                () -> assertEquals(15_000.0, ((ParsedStats) buckets.get("toyota").getAggregations().asList().get(0)).getMax()),
                () -> assertEquals(13_500.0, ((ParsedStats) buckets.get("toyota").getAggregations().asList().get(0)).getAvg()),
                () -> assertEquals(12_000.0, ((ParsedStats) buckets.get("toyota").getAggregations().asList().get(0)).getMin()),
                () -> assertEquals(27_000.0, ((ParsedStats) buckets.get("toyota").getAggregations().asList().get(0)).getSum())
        );
    }
}
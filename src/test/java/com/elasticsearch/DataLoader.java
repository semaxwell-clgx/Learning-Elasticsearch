package com.elasticsearch;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

/*
 * Don't want to manually load data in kibana? Let this class
 * handle it for you. Just add the following to test classes:
 *
 *     @BeforeAll
 *     public static void beforeAll() {
 *         new DataLoader().loadAllData();
 *     }
 *     @AfterAll
 *     public static void afterAll() {
 *         new DataLoader().removeIndices();
 *     }
 *
 * Note: the indices are expected to not already exist in Kibana
 *
 */
public class DataLoader {

    private List<String> courses = List.of(
            "{\"name\": \"Accounting 101\", \"room\": \"E3\", \"professor\": {\"name\": \"Thomas Baszo\", \"department\": \"finance\", \"faculty_type\": \"part-time\", \"email\": \"baszot@onuni.com\"}, \"students_enrolled\": 27, \"course_publish_date\": \"2015-01-19\", \"course_description\": \"Act 101 is a course from the business school on the introduction to accounting that teaches students how to read and compose basic financial statements\"}",
            "{\"name\": \"Marketing 101\", \"room\": \"E4\", \"professor\": {\"name\": \"William Smith\", \"department\": \"finance\", \"faculty_type\": \"part-time\", \"email\": \"wills@onuni.com\"}, \"students_enrolled\": 18, \"course_publish_date\": \"2015-06-21\", \"course_description\": \"Mkt 101 is a course from the business school on the introduction to marketing that teaches students the fundamentals of market analysis, customer retention and online advertisements\"}",
            "{\"name\": \"Anthropology 230\", \"room\": \"G11\", \"professor\": {\"name\": \"Devin Cranford\", \"department\": \"history\", \"faculty_type\": \"full-time\", \"email\": \"devinc@onuni.com\"}, \"students_enrolled\": 22, \"course_publish_date\": \"2013-08-27\", \"course_description\": \"Ant 230 is an intermediate course on human societies and cultures and their development. A focus on the Mayans civilization is rooted in this course\"}",
            "{\"name\": \"Computer Science 101\", \"room\": \"C12\", \"professor\": {\"name\": \"Gregg Payne\", \"department\": \"engineering\", \"faculty_type\": \"full-time\", \"email\": \"payneg@onuni.com\"}, \"students_enrolled\": 33, \"course_publish_date\": \"2013-08-27\", \"course_description\": \"CS 101 is a first year computer science introduction teaching fundamental data structures and alogirthms using python. \"}",
            "{\"name\": \"Theatre 410\", \"room\": \"T18\", \"professor\": {\"name\": \"Sebastian Hern\", \"department\": \"art\", \"faculty_type\": \"part-time\"}, \"students_enrolled\": 47, \"course_publish_date\": \"2013-01-27\", \"course_description\": \"Tht 410 is an advanced elective course disecting the various plays written by shakespere during the 16th century\"}",
            "{\"name\": \"Cost Accounting 400\", \"room\": \"E7\", \"professor\": {\"name\": \"Bill Cage\", \"department\": \"accounting\", \"faculty_type\": \"full-time\", \"email\": \"cageb@onuni.com\"}, \"students_enrolled\": 31, \"course_publish_date\": \"2014-12-31\", \"course_description\": \"Cst Act 400 is an advanced course from the business school taken by final year accounting majors that covers the subject of business incurred costs and how to record them in financial statements\"}",
            "{\"name\": \"Computer Internals 250\", \"room\": \"C8\", \"professor\": {\"name\": \"Gregg Payne\", \"department\": \"engineering\", \"faculty_type\": \"part-time\", \"email\": \"payneg@onuni.com\"}, \"students_enrolled\": 33, \"course_publish_date\": \"2012-08-20\", \"course_description\": \"cpt Int 250 gives students an integrated and rigorous picture of applied computer science, as it comes to play in the construction of a simple yet powerful computer system. \"}",
            "{\"name\": \"Accounting Info Systems 350\", \"room\": \"E3\", \"professor\": {\"name\": \"Bill Cage\", \"department\": \"accounting\", \"faculty_type\": \"full-time\", \"email\": \"cageb@onuni.com\"}, \"students_enrolled\": 19, \"course_publish_date\": \"2014-05-15\", \"course_description\": \"Act Sys 350 is an advanced course providing students a practical understanding of an accounting system in database technology. Students will use MS Access to build a transaction ledger system\"}",
            "{\"name\": \"Tax Accounting 200\", \"room\": \"E7\", \"professor\": {\"name\": \"Thomas Baszo\", \"department\": \"finance\", \"faculty_type\": \"part-time\", \"email\": \"baszot@onuni.com\"}, \"students_enrolled\": 17, \"course_publish_date\": \"2016-06-15\", \"course_description\": \"Tax Act 200 is an intermediate course covering various aspects of tax law\"}",
            "{\"name\": \"Capital Markets 350\", \"room\": \"E3\", \"professor\": {\"name\": \"Thomas Baszo\", \"department\": \"finance\", \"faculty_type\": \"part-time\", \"email\": \"baszot@onuni.com\"}, \"students_enrolled\": 13, \"course_publish_date\": \"2016-01-11\", \"course_description\": \"This is an advanced course teaching crucial topics related to raising capital and bonds, shares and other long-term equity and debt financial instrucments\"}" );

    private String nestedUsersIndex = "{\"mappings\": {\"properties\": {\"user\": {\"type\": \"nested\"} } } }";
    private List<String> nestedUsers = List.of(
            "{\"group\" : \"fans\", \"user\" : [{\"first\" : \"John\", \"last\" :  \"Smith\"}, {\"first\" : \"Alice\", \"last\" :  \"White\"} ] }" );

    private String nestedDriversIndex = "{\"mappings\":{\"properties\":{\"driver\":{\"type\":\"nested\", \"properties\":{\"last_name\":{\"type\":\"text\"}, \"vehicle\":{\"type\":\"nested\", \"properties\":{\"make\":{\"type\":\"text\"}, \"model\":{\"type\":\"text\"} } } } } } } }";
    private List<String> nestedDrivers = List.of(
            "{\"driver\":{\"last_name\":\"McQueen\", \"vehicle\":[{\"make\":\"Powell Motors\", \"model\":\"Canyonero\"}, {\"make\":\"Miller-Meteor\", \"model\":\"Ecto-1\"} ] } }",
            "{\"driver\":{\"last_name\":\"Hudson\", \"vehicle\":[{\"make\":\"Mifune\", \"model\":\"Mach Five\"}, {\"make\":\"Miller-Meteor\", \"model\":\"Ecto-1\"} ] } }" );

    private List<String> vehicles = List.of(
            "{ \"price\" : 10000, \"color\" : \"white\", \"make\" : \"honda\", \"sold\" : \"2016-10-28\", \"condition\": \"okay\"}",
            "{ \"price\" : 20000, \"color\" : \"white\", \"make\" : \"honda\", \"sold\" : \"2016-11-05\", \"condition\": \"new\" }",
            "{ \"price\" : 30000, \"color\" : \"green\", \"make\" : \"ford\", \"sold\" : \"2016-05-18\", \"condition\": \"new\" }",
            "{ \"price\" : 15000, \"color\" : \"blue\", \"make\" : \"toyota\", \"sold\" : \"2016-07-02\", \"condition\": \"good\" }",
            "{ \"price\" : 12000, \"color\" : \"green\", \"make\" : \"toyota\", \"sold\" : \"2016-08-19\" , \"condition\": \"good\"}",
            "{ \"price\" : 18000, \"color\" : \"red\", \"make\" : \"dodge\", \"sold\" : \"2016-11-05\", \"condition\": \"good\"  }",
            "{ \"price\" : 80000, \"color\" : \"red\", \"make\" : \"bmw\", \"sold\" : \"2016-01-01\", \"condition\": \"new\"  }",
            "{ \"price\" : 25000, \"color\" : \"blue\", \"make\" : \"ford\", \"sold\" : \"2016-08-22\", \"condition\": \"new\"  }",
            "{ \"price\" : 10000, \"color\" : \"gray\", \"make\" : \"dodge\", \"sold\" : \"2016-02-12\", \"condition\": \"okay\" }",
            "{ \"price\" : 19000, \"color\" : \"red\", \"make\" : \"dodge\", \"sold\" : \"2016-02-12\", \"condition\": \"good\" }",
            "{ \"price\" : 20000, \"color\" : \"red\", \"make\" : \"chevrolet\", \"sold\" : \"2016-08-15\", \"condition\": \"good\" }",
            "{ \"price\" : 13000, \"color\" : \"gray\", \"make\" : \"chevrolet\", \"sold\" : \"2016-11-20\", \"condition\": \"okay\" }",
            "{ \"price\" : 12500, \"color\" : \"gray\", \"make\" : \"dodge\", \"sold\" : \"2016-03-09\", \"condition\": \"okay\" }",
            "{ \"price\" : 35000, \"color\" : \"red\", \"make\" : \"dodge\", \"sold\" : \"2016-04-10\", \"condition\": \"new\" }",
            "{ \"price\" : 28000, \"color\" : \"blue\", \"make\" : \"chevrolet\", \"sold\" : \"2016-08-15\", \"condition\": \"new\" }",
            "{ \"price\" : 30000, \"color\" : \"gray\", \"make\" : \"bmw\", \"sold\" : \"2016-11-20\", \"condition\": \"good\" }" );

    private RestHighLevelClient client;

    void loadAllData() {
        this.client = getClient();

        loadBulkData(courses, "courses");

        createIndex(nestedUsersIndex, "my-users");
        loadData(nestedUsers, "my-users");

        createIndex(nestedDriversIndex, "drivers");
        loadData(nestedDrivers, "drivers");

        loadBulkData(vehicles, "vehicles");
    }

    private void createIndex(String command, String index) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(index);
        createIndexRequest.source(command, XContentType.JSON);
        try {
            client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void loadData(List<String> data, String index) {
        IntStream.rangeClosed(0, data.size()-1).forEach(n -> {
            IndexRequest indexRequest = new IndexRequest(index);
            indexRequest.source(data.get(n), XContentType.JSON);
            indexRequest.id(Integer.toString(++n));

            try {
                client.index(indexRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private void loadBulkData(List<String> data, String index) {
        BulkRequest bulkRequest = new BulkRequest();
        IntStream.rangeClosed(0, data.size()-1).forEach(n -> {
            bulkRequest.add(new IndexRequest(index)
                    .source(data.get(n), XContentType.JSON)
                    .id(Integer.toString(++n))
            );
        });
        try {
            client.bulk(bulkRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void removeIndices() {
        RestHighLevelClient client = getClient();
        List.of("courses", "my-users", "vehicles", "drivers").stream()
                .forEach(i -> {
                    try {
                        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(i);
                        client.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    private RestHighLevelClient getClient() {
        ClientConfiguration clientConfiguration
                = ClientConfiguration.builder()
                .connectedTo("localhost:9200")
                .build();
        return RestClients.create(clientConfiguration).rest();
    }
}
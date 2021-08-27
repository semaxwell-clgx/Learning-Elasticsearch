package com.elasticsearch;

import com.elasticsearch.domain.courses.Course;
import com.elasticsearch.domain.driver.DriverContainer;
import com.elasticsearch.domain.groups.Group;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.search.SearchResponse;

import java.util.List;
import java.util.stream.Collectors;

public class TestUtils {

    private static ObjectMapper mapper = new ObjectMapper();

    /*
     * Map SearchResponse Hits to Courses
     */
    static List<Course> extractCourses(SearchResponse searchResponse) {

        return List.of(searchResponse.getHits().getHits())
                .stream()
                .map(h -> h.getSourceAsString())
                .map(h -> {
                    try {
                        return mapper.readValue(h, Course.class);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return null;
                }).collect(Collectors.toList());
    }

    /*
     * Map SearchResponse Hits to Groups
     */
    static List<Group> extractGroups(SearchResponse searchResponse) {
        return List.of(searchResponse.getHits().getHits())
                .stream()
                .map(h -> h.getSourceAsString())
                .map(h -> {
                    try {
                        return mapper.readValue(h, Group.class);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return null;
                }).collect(Collectors.toList());
    }

    /*
     * Map SearchResponse Hits to DriverContainers
     */
    static List<DriverContainer> extractDrivers(SearchResponse searchResponse) {
        return List.of(searchResponse.getHits().getHits())
                .stream()
                .map(h -> h.getSourceAsString())
                .map(h -> {
                    try {
                        return mapper.readValue(h, DriverContainer.class);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    return null;
                }).collect(Collectors.toList());
    }
}

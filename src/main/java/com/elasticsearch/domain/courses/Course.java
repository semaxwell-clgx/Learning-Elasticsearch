package com.elasticsearch.domain.courses;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Course {

    private String name;
    private String room;
    private Professor professor;
    @JsonProperty("students_enrolled")
    private long studentsEnrolled;
    @JsonProperty("course_publish_date")
    private String coursePublishDate;
    @JsonProperty("course_description")
    private String courseDescription;

}

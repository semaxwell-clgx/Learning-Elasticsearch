package com.elasticsearch.domain.courses;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Professor {

    private String name;
    private String department;
    @JsonProperty("faculty_type")
    private String facultyType;
    private String email;
}

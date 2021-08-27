package com.elasticsearch.domain.groups;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
public class Group {
    private String Group;
    @JsonProperty("user")
    private List<User> users;
}

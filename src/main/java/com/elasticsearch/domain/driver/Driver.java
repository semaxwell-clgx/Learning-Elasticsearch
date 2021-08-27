package com.elasticsearch.domain.driver;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
public class Driver {

    @JsonProperty("last_name")
    private String lastName;
    @JsonProperty("vehicle")
    private List<Vehicle> vehicles;
}

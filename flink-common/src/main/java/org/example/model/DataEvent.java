package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataEvent {

    private String id1;

    private String id2;

    private String id3;

    private Date eventTime;

    private int value;
}

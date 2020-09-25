package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
public class DataEventHistory{

    public DataEventHistory(DataEvent value) {
        this.id = value.getId();
        this.eventDate = value.getEventTime().toLocalDate();
        this.hn = String.format("h%s", value.getEventTime().getHour());
        this.lastValue = value.getValue();
    }

    private String id;
    private LocalDate eventDate;
    private String hn;
    private Integer lastValue;
}

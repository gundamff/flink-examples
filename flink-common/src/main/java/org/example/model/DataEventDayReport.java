package org.example.model;

import java.time.LocalDate;
import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhangpeng
 * @date 2020/09/11
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DataEventDayReport {

    /**
     * @param value
     */
    public DataEventDayReport(DataEvent value) {
        this.id = value.getId();
        int minute = (1+(value.getEventTime().getMinute()/5)) * 5;
        this.fiveMinutes = value.getEventTime().withHour(0).withMinute(minute).withSecond(0).withNano(0);
        this.hn = String.format("h%s", value.getEventTime().getHour());
        this.lastValue = value.getValue();
    }

    private String id;

    private LocalDateTime fiveMinutes;

    private String hn;

    private Integer lastValue;

    public static void main(String[] args) {
        int m = 16;
        System.out.println();
    }

}

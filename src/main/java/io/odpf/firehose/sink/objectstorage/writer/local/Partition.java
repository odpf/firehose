package io.odpf.firehose.sink.objectstorage.writer.local;

import io.odpf.firehose.sink.objectstorage.Constants;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;


/**
 * Partition generate partition formatted string topic/dt=yyyy-MM-dd/hr=HH .
 */
@AllArgsConstructor
@Data
public class Partition {
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH");

    private String topic;
    private Instant timestamp;
    private PartitionConfig partitionConfig;

    private String getDate() {
        LocalDate localDate = LocalDateTime.ofInstant(timestamp, ZoneId.of(partitionConfig.getZone())).toLocalDate();
        return DATE_FORMATTER.format(localDate);
    }

    private String getHour() {
        LocalTime localTime = LocalDateTime.ofInstant(timestamp, ZoneId.of(partitionConfig.getZone())).toLocalTime();
        return TIME_FORMATTER.format(localTime);
    }


    public String getDatetimePathWithoutPrefix() {
        String datePart = getDate();
        String hourPart = getHour();
        return getPath(datePart, hourPart);
    }

    private String getPath(String datePart, String hourPart) {
        switch (partitionConfig.getPartitioningType()) {
            case DAY:
                return String.format("%s", datePart);
            case HOUR:
                return String.format("%s/%s", datePart, hourPart);
            default:
                throw new IllegalArgumentException();
        }
    }

    private String getDatetimePath() {
        String datePart = getDate();
        String dateSegment = String.format("%s%s", partitionConfig.getDatePrefix(), datePart);

        String hourPart = getHour();
        String hourSegment = String.format("%s%s", partitionConfig.getHourPrefix(), hourPart);

        return getPath(dateSegment, hourSegment);
    }


    private String getPartitionPath() {
        if (partitionConfig.getPartitioningType() == Constants.PartitioningType.NONE) {
            return topic;
        }

        String datetimePartition = getDatetimePath();
        return String.format("%s/%s", topic, datetimePartition);
    }

    /**
     * Create PartitionPath object from partition path.
     *
     * @param partitionPath   partition path is a relative path, it should only contains partition segment, it should not contains file name or base directory path
     * @param partitionConfig
     * @return
     */
    public static Partition parseFrom(String partitionPath, PartitionConfig partitionConfig) {
        Path path = Paths.get(partitionPath);
        String topic = path.getName(0).toString();
        if (partitionConfig.getPartitioningType() == Constants.PartitioningType.NONE) {
            return new Partition(topic, null, partitionConfig);
        }

        String datePath = path.getName(1).toString().replace(partitionConfig.getDatePrefix(), "");
        ZonedDateTime zonedDateTime = LocalDate.parse(datePath, DATE_FORMATTER).atStartOfDay(ZoneId.of(partitionConfig.getZone()));
        if (partitionConfig.getPartitioningType() == Constants.PartitioningType.DAY) {
            return new Partition(topic, zonedDateTime.toInstant(), partitionConfig);
        }

        String timePath = path.getName(2).toString().replace(partitionConfig.getHourPrefix(), "");
        LocalTime localTime = LocalTime.parse(timePath, TIME_FORMATTER);
        zonedDateTime = zonedDateTime.plusHours(localTime.getHour());
        if (partitionConfig.getPartitioningType() == Constants.PartitioningType.HOUR) {
            return new Partition(topic, zonedDateTime.toInstant(), partitionConfig);
        }

        throw new IllegalArgumentException();
    }

    public Path getPath() {
        return Paths.get(getPartitionPath());
    }

    @Override
    public String toString() {
        return getPartitionPath();
    }
}

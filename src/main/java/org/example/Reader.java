package org.example;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Reader {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Sensor> sensorData = env.addSource(new SensorSource());
        sensorData.map(r -> {
            return new Sensor(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0));
        }).
                keyBy(r -> r.id).
                timeWindow(Time.seconds(5)).
                apply((s, timeWindow, iterable, collector) -> {
                    iterable.
                    double s = 0;
                    for (Sensor sensor : iterable) {
                        s += sensor.temperature;
                    }
                    return s/ iterable.
                });

    }
}

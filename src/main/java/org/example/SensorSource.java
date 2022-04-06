package org.example;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

public class SensorSource extends RichParallelSourceFunction<Sensor> {
    private boolean running = true;

    @Override
    public void run(SourceContext<Sensor> srcCtx) throws Exception {
        Random rand = new Random();
        String[] sensorIds = new String[10];
        double[] curFTemp = new double[10];
        for (int i = 0; i < 10; i++) {
            sensorIds[i] = "sensor_" + i; // 初始化sensor id
            curFTemp[i] = 65 + (rand.nextGaussian() * 20);  // 设置温度
        }

        while (running) {
            long curTime = Calendar.getInstance().getTimeInMillis();
            for (int i = 0; i < 10; i++) {
                curFTemp[i] += rand.nextGaussian() * 0.5;
                srcCtx.collect(new Sensor(sensorIds[i], curTime, curFTemp[i]));
            }
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}

package org.example;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.example.FlinkStreamingApp;

public class BenchmarkingMapper extends RichMapFunction<FlinkStreamingApp.Employee, FlinkStreamingApp.Employee> {
    private transient long startTime;
    private transient long processedCount;

    @Override
    public void open(Configuration parameters) throws Exception {
        startTime = System.currentTimeMillis();
        processedCount = 0;
    }

    @Override
    public FlinkStreamingApp.Employee map(FlinkStreamingApp.Employee employee) throws Exception {
        processedCount++;
        if (processedCount % 100 == 0) {
            long currentTime = System.currentTimeMillis();
            long elapsedTime = currentTime - startTime;
            System.out.println("Processed " + processedCount + " employees in " + elapsedTime + " ms");
        }
        return employee;
    }
}

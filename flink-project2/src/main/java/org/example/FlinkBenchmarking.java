package org.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class FlinkBenchmarking {

    public static class BenchmarkingMapper extends RichMapFunction<Employee, Employee> {
        private transient long startTime;
        private transient long processedCount;

        @Override
        public void open(Configuration parameters) throws Exception {
            startTime = System.currentTimeMillis();
            processedCount = 0;
        }

        @Override
        public Employee map(Employee employee) throws Exception {
            processedCount++;
            if (processedCount % 100 == 0) {
                long currentTime = System.currentTimeMillis();
                long elapsedTime = currentTime - startTime;
                System.out.println("Processed " + processedCount + " employees in " + elapsedTime + " ms");
            }
            return employee;
        }
    }

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Add a source to generate random employee data
        DataStream<Employee> employeeStream = env.addSource(new RandomEmployeeSource())
                .map(new BenchmarkingMapper());

        // Execute the Flink job
        env.execute("Flink Benchmarking Application");
    }

    // Source function to generate random employee data
    public static class RandomEmployeeSource implements SourceFunction<Employee> {
        private volatile boolean isRunning = true;
        private final List<String> names = Arrays.asList("John", "Jane", "Alex", "Emily", "Chris");
        private final List<String> surnames = Arrays.asList("Smith", "Doe", "Johnson", "Brown", "Davis");
        private final List<String> departments = Arrays.asList("HR", "IT", "Sales", "Finance", "Marketing");

        @Override
        public void run(SourceContext<Employee> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                String name = names.get(random.nextInt(names.size()));
                String surname = surnames.get(random.nextInt(surnames.size()));
                String department = departments.get(random.nextInt(departments.size()));
                int yearOfBirth = 1960 + random.nextInt(40);
                int nrOfYears = random.nextInt(40);
                long timestamp = System.currentTimeMillis();
                Employee employee = new Employee(name, surname, department, yearOfBirth, nrOfYears, timestamp);
                ctx.collect(employee);
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    // Employee class
    public static class Employee {
        public String name;
        public String surname;
        public String department;
        public int yearOfBirth;
        public int nrOfYears;
        public long timestamp;

        public Employee(String name, String surname, String department, int yearOfBirth, int nrOfYears, long timestamp) {
            this.name = name;
            this.surname = surname;
            this.department = department;
            this.yearOfBirth = yearOfBirth;
            this.nrOfYears = nrOfYears;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "Employee{" +
                    "name='" + name + '\'' +
                    ", surname='" + surname + '\'' +
                    ", department='" + department + '\'' +
                    ", yearOfBirth=" + yearOfBirth +
                    ", nrOfYears=" + nrOfYears +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
}

//package org.example;
//
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//
//import java.util.Random;
//import java.util.Arrays;
//import java.util.List;
//
//public class FlinkStreamingApp {
//
//    public static void main(String[] args) throws Exception {
//        // Set up the execution environment
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // Add a source to generate random employee data
//        env.addSource(new RandomEmployeeSource())
//                // Process each employee
//                .filter(new EmployeeAgeFilterFunction())
//                // Print the results to the console
//                .print();
//
//        // Execute the program
//        env.execute("Flink Streaming Application");
//    }
//
//    // Source function to generate random employee data
//    public static class RandomEmployeeSource implements SourceFunction<Employee> {
//        private volatile boolean isRunning = true;
//        private final List<String> names = Arrays.asList("John", "Jane", "Alex", "Emily", "Chris");
//        private final List<String> surnames = Arrays.asList("Smith", "Doe", "Johnson", "Brown", "Davis");
//        private final List<String> departments = Arrays.asList("HR", "IT", "Sales", "Finance", "Marketing");
//
//        @Override
//        public void run(SourceContext<Employee> ctx) throws Exception {
//            Random random = new Random();
//            while (isRunning) {
//                String name = names.get(random.nextInt(names.size()));
//                String surname = surnames.get(random.nextInt(surnames.size()));
//                String department = departments.get(random.nextInt(departments.size()));
//                int yearOfBirth = 1960 + random.nextInt(40); // random year between 1960 and 2000
//                int nrOfYears = random.nextInt(40); // random number of years between 0 and 40
//                Employee employee = new Employee(name, surname, department, yearOfBirth, nrOfYears);
//                ctx.collect(employee);
//                Thread.sleep(500); // generate a new employee every 500ms
//            }
//        }
//
//        @Override
//        public void cancel() {
//            isRunning = false;
//        }
//    }
//
//    // Employee class
//    public static class Employee {
//        public String name;
//        public String surname;
//        public String department;
//        public int yearOfBirth;
//        public int nrOfYears;
//
//        public Employee(String name, String surname, String department, int yearOfBirth, int nrOfYears) {
//            this.name = name;
//            this.surname = surname;
//            this.department = department;
//            this.yearOfBirth = yearOfBirth;
//            this.nrOfYears = nrOfYears;
//        }
//
//        @Override
//        public String toString() {
//            return "Employee{" +
//                    "name='" + name + '\'' +
//                    ", surname='" + surname + '\'' +
//                    ", department='" + department + '\'' +
//                    ", yearOfBirth=" + yearOfBirth +
//                    ", nrOfYears=" + nrOfYears +
//                    '}';
//        }
//    }
//
//    // Filter function to filter employees based on age > 25
//    public static class EmployeeAgeFilterFunction implements FilterFunction<Employee> {
//        @Override
//        public boolean filter(Employee employee) throws Exception {
//            int currentYear = 2024; // you can dynamically get the current year if needed
//            int age = currentYear - employee.yearOfBirth;
//            if (age > 25) {
//                System.out.println("This employee has the age bigger than 25: " + employee);
//                return true;
//            }
//            return false;
//        }
//    }
//}
//----------------------------
package org.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

public class FlinkStreamingApp {

    // Maps to store the results
    private static ConcurrentHashMap<String, Employee> raiseEligibleMap = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Employee> promotionEligibleMap = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set time characteristic to Event Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Add a source to generate random employee data
        DataStream<Employee> employeeStream = env.addSource(new RandomEmployeeSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Employee>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Employee>() {
                            @Override
                            public long extractTimestamp(Employee element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        // Filter employees older than 25 and store them for analysis
        DataStream<Employee> filteredEmployees = employeeStream
                .filter(employee -> {
                    int currentYear = 2024;
                    int age = currentYear - employee.yearOfBirth;
                    return age > 25;
                })
                .map(new MapFunction<Employee, Employee>() {
                    @Override
                    public Employee map(Employee employee) {
                        System.out.println("This employee has the age bigger than 25: " + employee);
                        return employee;
                    }
                });

        // Aggregate: Calculate average age per department
        filteredEmployees
                .keyBy(employee -> employee.department)
                .timeWindow(Time.seconds(10))
                .apply(new AverageAgePerDepartment())
                .print();

        // Aggregate: Total years worked per department
        filteredEmployees
                .keyBy(employee -> employee.department)
                .timeWindow(Time.seconds(10))
                .reduce(new TotalYearsWorked())
                .map(new MapFunction<Employee, String>() {
                    @Override
                    public String map(Employee employee) {
                        return "Total years worked in department " + employee.department + " is " + employee.nrOfYears;
                    }
                })
                .print();

        // Stream for employees eligible for a raise
        DataStream<Employee> raiseEligibleEmployees = filteredEmployees
                .filter(employee -> employee.nrOfYears > 2)
                .map(new MapFunction<Employee, Employee>() {
                    @Override
                    public Employee map(Employee employee) {
                        raiseEligibleMap.put(employee.name + employee.surname, employee);
                        System.out.println("Employee eligible for a raise: " + employee);
                        return employee;
                    }
                });

        // Stream for employees eligible for a promotion
        DataStream<Employee> promotionEligibleEmployees = filteredEmployees
                .filter(employee -> employee.nrOfYears > 3 && (2024 - employee.yearOfBirth) > 30)
                .map(new MapFunction<Employee, Employee>() {
                    @Override
                    public Employee map(Employee employee) {
                        promotionEligibleMap.put(employee.name + employee.surname, employee);
                        System.out.println("Employee eligible for a promotion: " + employee);
                        return employee;
                    }
                });

        // Stream for employees eligible for a raise
        DataStream<Integer> raiseEligibleCountStream = employeeStream
                .filter(employee -> employee.nrOfYears > 2)
                .map(new MapFunction<Employee, Integer>() {
                    @Override
                    public Integer map(Employee employee) {
                        raiseEligibleMap.put(employee.name + employee.surname, employee);
                        System.out.println("Employee eligible for a raise: " + employee);
                        return 1;
                    }
                });

// Stream for employees eligible for a promotion
        DataStream<Integer> promotionEligibleCountStream = employeeStream
                .filter(employee -> employee.nrOfYears > 3 && (2024 - employee.yearOfBirth) > 30)
                .map(new MapFunction<Employee, Integer>() {
                    @Override
                    public Integer map(Employee employee) {
                        promotionEligibleMap.put(employee.name + employee.surname, employee);
                        System.out.println("Employee eligible for a promotion: " + employee);
                        return 1;
                    }
                });

        // Aggregate the counts every 1 minute
        DataStream<String> aggregatedCounts = raiseEligibleCountStream
                .union(promotionEligibleCountStream)
                .timeWindowAll(Time.minutes(1))
                .sum(0)  // Sum the counts
                .map(new MapFunction<Integer, String>() {
                    @Override
                    public String map(Integer count) {
                        return "Total number of employees eligible for raise and promotion: " + count;
                    }
                });

// Print the aggregated counts
        aggregatedCounts.print();
        // Execute the Flink job
        env.execute("Flink Streaming Application");
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

    // Class to hold eligibility counts
    public static class EligibilityCounts {
        public int raiseEligibleCount;
        public int promotionEligibleCount;

        public EligibilityCounts(int raiseEligibleCount, int promotionEligibleCount) {
            this.raiseEligibleCount = raiseEligibleCount;
            this.promotionEligibleCount = promotionEligibleCount;
        }
    }

    // Window function to calculate average age per department
    public static class AverageAgePerDepartment implements WindowFunction<Employee, String, String, TimeWindow> {
        @Override
        public void apply(String department, TimeWindow window, Iterable<Employee> elements, Collector<String> out) throws Exception {
            int count = 0;
            int totalAge = 0;
            int currentYear = 2024;

            for (Employee employee : elements) {
                int age = currentYear - employee.yearOfBirth;
                totalAge += age;
                count++;
            }

            if (count > 0) {
                double averageAge = (double) totalAge / count;
                out.collect("Average age for department " + department + " is " + averageAge);
            }
        }
    }

    // Reduce function to calculate total years worked per department
    public static class TotalYearsWorked implements ReduceFunction<Employee> {
        @Override
        public Employee reduce(Employee e1, Employee e2) throws Exception {
            e1.nrOfYears += e2.nrOfYears;
            return e1;
        }
    }
}

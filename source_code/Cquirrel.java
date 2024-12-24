package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class Cquirrel {
    public static final OutputTag<String> LI_TAG = new OutputTag<String>("LI") {
    };
    public static final OutputTag<String> OR_TAG = new OutputTag<String>("OR") {
    };
    public static final OutputTag<String> CU_TAG = new OutputTag<String>("CU") {
    };

    public static void main(String[] args) throws Exception {
        System.out.println("Starting the job");

        // Get arguments
        String inputPath = args[0];
        String outputPath = args[1];

        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setParallelism(4);

        // Read CSV file as DataStream
        DataStream<String> inputStream = env.readTextFile(inputPath);

        // Process the input and route to side outputs
        SingleOutputStreamOperator<String> mainStream = inputStream.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                String tableName = value.substring(1, 3);
                value = value.charAt(0) + value.substring(3);

                switch (tableName) {
                    case "LI":
                        // Emit to the LI side output
                        ctx.output(LI_TAG, value);
                        break;
                    case "OR":
                        // Emit to the OR side output
                        ctx.output(OR_TAG, value);
                        break;
                    case "CU":
                        // Emit to the CU side output
                        ctx.output(CU_TAG, value);
                        break;
                    default:
                        // Optionally handle unexpected cases
                        out.collect(value);
                        break;

                }
            }
        });


        // Get the side output. Map the string to customized object
        DataStream<LineItem> lineItemStream = mainStream.getSideOutput(LI_TAG).map(new MapFunction<String, LineItem>() {
            @Override
            public LineItem map(String value) throws Exception {
                String[] fields = value.split("\\|");
                char insertionMark = fields[0].charAt(0);
                fields[0] = fields[0].substring(1);

                return new LineItem(
                        insertionMark == '+',
                        Integer.parseInt(fields[0]),                                    // l_orderkey
                        Double.parseDouble(fields[5]),                                  // l_extendedprice
                        Double.parseDouble(fields[6]),                                  // l_discount
                        LocalDate.parse(fields[10], DateTimeFormatter.ISO_LOCAL_DATE),  // l_shipdate
                        Integer.parseInt(fields[3])                                     // l_lineNumber
                );
            }
        });
        DataStream<Order> orderStream = mainStream.getSideOutput(OR_TAG).map(new MapFunction<String, Order>() {
            @Override
            public Order map(String value) throws Exception {
                String[] fields = value.split("\\|");
                char insertionMark = fields[0].charAt(0);
                fields[0] = fields[0].substring(1);

                return new Order(
                        insertionMark == '+',
                        Integer.parseInt(fields[0]),                                    // o_orderkey
                        Integer.parseInt(fields[1]),                                    // o_custkey
                        LocalDate.parse(fields[4], DateTimeFormatter.ISO_LOCAL_DATE),   // o_orderdate
                        Integer.parseInt(fields[7])                                     // o_shippriority
                );
            }
        });
        DataStream<Customer> customerStream = mainStream.getSideOutput(CU_TAG).map(new MapFunction<String, Customer>() {
            @Override
            public Customer map(String value) throws Exception {
                String[] fields = value.split("\\|");
                char insertionMark = fields[0].charAt(0);
                fields[0] = fields[0].substring(1);

                return new Customer(
                        insertionMark == '+',
                        Integer.parseInt(fields[0]),        // c_custkey
                        fields[6]                           // c_mktsegment

                );
            }
        });


        // Filter the stream
        lineItemStream = lineItemStream.filter(value -> value.getShipDate().isAfter(LocalDate.parse("1995-03-13", DateTimeFormatter.ISO_LOCAL_DATE)));
        orderStream = orderStream.filter(value -> value.getOrderDate().isBefore(LocalDate.parse("1995-03-13", DateTimeFormatter.ISO_LOCAL_DATE)));
        customerStream = customerStream.filter(value -> value.getMktSegment().equals("AUTOMOBILE"));


        // Connect order(parent) and customer(child), keyed by the common key (CustKey). Emit join result to downstream.
        DataStream<Joint_Order_Cust> joint_Order_Cust_Stream = orderStream.keyBy(Order::getCustKey)
                .connect(customerStream.keyBy(Customer::getCustKey))
                .process(new CoProcessFunction<Order, Customer, Joint_Order_Cust>() {
                    private transient MapState<Integer, Order> parentTuplesState;   // storing a map of parent tuples (the key of map is OrderKey)
                    private transient ValueState<Customer> childTupleState;         // storing the child tuple

                    @Override
                    public void open(Configuration parameters) {
                        parentTuplesState = getRuntimeContext().getMapState(new MapStateDescriptor<>("parent_records_joint_order_cust", Integer.class, Order.class));
                        childTupleState = getRuntimeContext().getState(new ValueStateDescriptor<>("child_record_joint_order_cust", Customer.class));
                    }

                    // Process parent tuple
                    @Override
                    public void processElement1(Order parentTuple, CoProcessFunction<Order, Customer, Joint_Order_Cust>.Context context,
                                                Collector<Joint_Order_Cust> collector) throws Exception {

                        // Process insertion
                        if (parentTuple.isInsertion()) {
                            // Insert the input tuple into the map of parentTuplesState
                            parentTuplesState.put(parentTuple.getOrderKey(), parentTuple);

                            // If the child of the input tuple is alive (i.e. the child tuple exists), emit the joint result (insertion)
                            if (this.childTupleState.value() != null) {
                                collector.collect(new Joint_Order_Cust(true, parentTuple.getOrderKey(), parentTuple.getOrderDate(), parentTuple.getShipPriority()));
                            }
                        }

                        // Process deletion
                        else {
                            // Delete the input tuple from the map of parentTuplesState
                            parentTuplesState.remove(parentTuple.getOrderKey());

                            // If the child of the input tuple is alive (i.e. the child tuple exists), emit the joint result (deletion)
                            if (this.childTupleState.value() != null) {
                                collector.collect(new Joint_Order_Cust(false, parentTuple.getOrderKey(), parentTuple.getOrderDate(), parentTuple.getShipPriority()));
                            }
                        }
                    }

                    // Process child tuple
                    @Override
                    public void processElement2(Customer childTuple, CoProcessFunction<Order, Customer, Joint_Order_Cust>.Context context,
                                                Collector<Joint_Order_Cust> collector) throws Exception {

                        // Process insertion
                        if (childTuple.isInsertion()) {
                            // Add the input tuple to childTupleState (i.e. make it alive)
                            childTupleState.update(childTuple);

                            // Emit a join result (insertion) for each parent tuples (if any) corresponding to the input tuple
                            for (Order parentTuple : parentTuplesState.values()) {
                                collector.collect(new Joint_Order_Cust(true, parentTuple.getOrderKey(), parentTuple.getOrderDate(), parentTuple.getShipPriority()));
                            }
                        }

                        // Process deletion
                        else {
                            // Remove the value in childTupleState (i.e. make it dead)
                            childTupleState.clear();

                            // Emit a join result (deletion) for each parent tuples (if any) corresponding to the input tuple
                            for (Order parentTuple : parentTuplesState.values()) {
                                collector.collect(new Joint_Order_Cust(false, parentTuple.getOrderKey(), parentTuple.getOrderDate(), parentTuple.getShipPriority()));
                            }
                        }
                    }
                });


        // Connect lineitem and order, keyed by the common key (OrderKey). Emit join result to downstream.
        DataStream<Joint_LineItem_Order> joint_LineItem_Order_Stream = lineItemStream.keyBy(LineItem::getOrderKey)
                .connect(joint_Order_Cust_Stream.keyBy(Joint_Order_Cust::getOrderKey))
                .process(new CoProcessFunction<LineItem, Joint_Order_Cust, Joint_LineItem_Order>() {
                    // Keyed state to track total clicks per user
                    private transient MapState<Integer, LineItem> parentTuplesState;            // storing a list of parent records by key
                    private transient ValueState<Joint_Order_Cust> childTupleState;   // storing the child record by key

                    @Override
                    public void open(Configuration parameters) {
                        parentTuplesState = getRuntimeContext().getMapState(new MapStateDescriptor<>("parent_records_joint_lineitem_order", Integer.class, LineItem.class));
                        childTupleState = getRuntimeContext().getState(new ValueStateDescriptor<>("child_record_joint_lineitem_order", Joint_Order_Cust.class));
                    }

                    // Process parent tuple
                    @Override
                    public void processElement1(LineItem parentTuple, CoProcessFunction<LineItem, Joint_Order_Cust, Joint_LineItem_Order>.Context context,
                                                Collector<Joint_LineItem_Order> collector) throws Exception {

                        // Process Insertion
                        if (parentTuple.isInsertion()) {
                            // Insert the input tuple into the map of parentTuplesState
                            parentTuplesState.put(parentTuple.getLineNumber(), parentTuple);

                            // If the child of the input tuple is alive (i.e. the child tuple exists), emit the joint result (insertion)
                            if (childTupleState.value() != null) {
                                Joint_Order_Cust childTuple = childTupleState.value();
                                collector.collect(new Joint_LineItem_Order(true, childTuple.getOrderKey(), childTuple.getOrderDate(), childTuple.getShipPriority(),
                                        parentTuple.getExtendedPrice(), parentTuple.getDiscount()));
                            }
                        }

                        // Process Deletion
                        else {
                            // Remove the input tuple into the map of parentTuplesState
                            parentTuplesState.remove(parentTuple.getLineNumber());

                            // If the child of the input tuple is alive (i.e. the child tuple exists), emit the joint result (deletion)
                            if (childTupleState.value() != null) {
                                Joint_Order_Cust childTuple = childTupleState.value();
                                collector.collect(new Joint_LineItem_Order(false, childTuple.getOrderKey(), childTuple.getOrderDate(), childTuple.getShipPriority(),
                                        parentTuple.getExtendedPrice(), parentTuple.getDiscount()));
                            }
                        }
                    }

                    // Process child tuple
                    @Override
                    public void processElement2(Joint_Order_Cust childTuple, CoProcessFunction<LineItem, Joint_Order_Cust, Joint_LineItem_Order>.Context context,
                                                Collector<Joint_LineItem_Order> collector) throws Exception {

                        // Process insertion
                        if (childTuple.isInsertion()) {
                            // Add the input tuple to childTupleState (i.e. make it alive)
                            childTupleState.update(childTuple);

                            // Emit a join result for each parent tuples (if any) corresponding to the input tuple (insertion)
                            for (LineItem parentTuple : parentTuplesState.values()) {
                                collector.collect(new Joint_LineItem_Order(true, childTuple.getOrderKey(), childTuple.getOrderDate(), childTuple.getShipPriority(),
                                        parentTuple.getExtendedPrice(), parentTuple.getDiscount()));
                            }
                        }

                        // Process deletion
                        else {
                            // Remove the value in childTupleState (i.e. make it dead)
                            childTupleState.clear();

                            // Emit a join result for each parent tuples (if any) corresponding to the input tuple (deletion)
                            for (LineItem parentTuple : parentTuplesState.values()) {
                                collector.collect(new Joint_LineItem_Order(false, childTuple.getOrderKey(), childTuple.getOrderDate(), childTuple.getShipPriority(),
                                        parentTuple.getExtendedPrice(), parentTuple.getDiscount()));
                            }


                        }
                    }
                });


        // Aggregate the result in the last output joint stream
        DataStream<JointResult> joint_Result_Stream = joint_LineItem_Order_Stream.keyBy(Joint_LineItem_Order::getOrderKey)
                .process(new KeyedProcessFunction<Integer, Joint_LineItem_Order, JointResult>() {
                    private transient ValueState<Double> sumState;

                    @Override
                    public void open(Configuration parameters) {
                        sumState = getRuntimeContext().getState(new ValueStateDescriptor<>("sumState", Double.class));
                    }

                    @Override
                    public void processElement(Joint_LineItem_Order tuple, KeyedProcessFunction<Integer, Joint_LineItem_Order, JointResult>.Context context,
                                               Collector<JointResult> collector) throws Exception {

                        // Get the current sum
                        Double currentSum = sumState.value();
                        if (currentSum == null) {
                            currentSum = 0.0;
                        }

                        // Update the current sum
                        if (tuple.isInsertion()) {
                            currentSum = currentSum + tuple.getExtendedPrice() * (1 - tuple.getDiscount());
                        } else {
                            currentSum = currentSum - tuple.getExtendedPrice() * (1 - tuple.getDiscount());
                        }
                        sumState.update(currentSum);

                        // Emit the final join result
                        collector.collect(new JointResult(tuple.getOrderKey(), tuple.getOrderDate(), tuple.getShipPriority(), currentSum));
                    }
                });


        // Write the final join result
        try {
            Path outputPathObj = new Path(outputPath);

            FileSystem fs = FileSystem.get(outputPathObj.toUri());

            if (fs.exists(outputPathObj)) {
                fs.delete(outputPathObj, false);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        joint_Result_Stream.writeAsText(outputPath, FileSystem.WriteMode.OVERWRITE);


        // Execute the job and record the running time
        long startTime = System.currentTimeMillis();

        env.execute("Cquirrel");

        long endTime = System.currentTimeMillis(); // Record end time
        long duration = endTime - startTime; // Calculate duration
        System.out.println("Job running time: " + duration + " milliseconds");
    }
}




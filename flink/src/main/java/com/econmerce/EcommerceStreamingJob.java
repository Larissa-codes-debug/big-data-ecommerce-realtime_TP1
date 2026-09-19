package com.ecommerce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class EcommerceStreamingJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        /*
         * Fonte temporária baseada em socket.
         *
         * Durante a integração final podemos substituir por Kafka,
         * mas para a demonstração acadêmica inicial o socket
         * permite testar o Event Time + Watermark + Window.
         */

        DataStream<String> input =
                env.socketTextStream(
                        "flume",
                        9999
                );

        ObjectMapper mapper = new ObjectMapper();

        DataStream<Event> events =
                input.map(
                        new MapFunction<String, Event>() {

                            @Override
                            public Event map(String value) throws Exception {

                                JsonNode json = mapper.readTree(value);

                                String eventType =
                                        json.get("event_type").asText();

                                String productId =
                                        json.has("product_id")
                                                ? json.get("product_id").asText()
                                                : "unknown";

                                long eventTime =
                                        java.time.Instant
                                                .parse(
                                                        json.get("event_time").asText()
                                                )
                                                .toEpochMilli();

                                return new Event(
                                        eventType,
                                        productId,
                                        eventTime
                                );
                            }
                        }
                );

        /*
         * WATERMARK
         *
         * Aceitamos eventos que chegarem até 10 segundos
         * fora de ordem.
         */

        WatermarkStrategy<Event> watermarkStrategy =
                WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(
                                Duration.ofSeconds(10)
                        )
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<Event>() {

                                    @Override
                                    public long extractTimestamp(
                                            Event event,
                                            long recordTimestamp) {

                                        return event.eventTime;
                                    }
                                }
                        );

        DataStream<Event> timestamped =
                events.assignTimestampsAndWatermarks(
                        watermarkStrategy
                );

        /*
         * SLIDING WINDOW
         *
         * Janela de 5 minutos
         * deslocamento de 1 minuto
         */

        timestamped
                .filter(
                        event ->
                                event.eventType.equals("click")
                )
                .keyBy(
                        event ->
                                event.productId
                )
                .window(
                        SlidingEventTimeWindows.of(
                                Time.minutes(5),
                                Time.minutes(1)
                        )
                )
                .process(
                        new ClickAlertProcessFunction()
                )
                .print();

        env.execute(
                "E-commerce Streaming - Click Alerts"
        );
    }

    public static class Event {

        public String eventType;

        public String productId;

        public long eventTime;

        public Event() {
        }

        public Event(
                String eventType,
                String productId,
                long eventTime
        ) {

            this.eventType = eventType;

            this.productId = productId;

            this.eventTime = eventTime;
        }
    }

    public static class ClickAlertProcessFunction
            extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<
                    Event,
                    String,
                    String,
                    TimeWindow> {

        @Override
        public void process(
                String productId,
                Context context,
                Iterable<Event> elements,
                Collector<String> out) {

            int count = 0;

            for (Event event : elements) {
                count++;
            }

            String result =
                    "ALERTA | produto="
                            + productId
                            + " | clicks="
                            + count
                            + " | janela="
                            + context.window().getStart()
                            + "-"
                            + context.window().getEnd();

            out.collect(result);
        }
    }
}
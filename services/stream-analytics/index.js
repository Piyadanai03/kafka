import { Kafka } from "kafkajs";
import express from "express";
import { SchemaRegistry, SchemaType } from "@kafkajs/confluent-schema-registry";
import { randomUUID } from "crypto";
import client from "prom-client";
import { createClient } from "redis";

const SERVICE_NAME = "analytics-svc";

//เชื่อมต่อ Schema Registry
const registry = new SchemaRegistry({
  host: "http://localhost:8081/apis/ccompat/v7",
});

//กำหนด "พิมพ์เขียว" (Schema) สำหรับ *ขาออก*
const metricsSchema = {
  type: SchemaType.AVRO,
  schema: JSON.stringify({
    type: "record",
    name: "OrdersPerMinute",
    namespace: "com.mycorp.metrics",
    fields: [
      { name: "timestamp", type: "string" },
      { name: "count", type: "int" },
      { name: "traceId", type: ["null", "string"], default: null },
    ],
  }),
};

let ordersInThisMinute = 0;

const kafka = new Kafka({
  clientId: SERVICE_NAME,
  brokers: ["localhost:9092"],
});
const consumer = kafka.consumer({ groupId: "analytics-processor-group" });
const producer = kafka.producer();

const redisClient = createClient({
  url: "redis://localhost:6379",
});
redisClient.on("error", (err) =>
  console.error(
    JSON.stringify({
      level: "ERROR",
      service: SERVICE_NAME,
      message: "Redis Client Error",
      error: err,
    })
  )
);

// เปิดการเก็บ Metrics เริ่มต้น
client.collectDefaultMetrics();

// 1. Gauge (มาตรวัด) สำหรับนับ Order ในหน้าต่างปัจจุบัน (ค่าจะขึ้น/ลง และ Reset)
const ordersInWindowGauge = new client.Gauge({
  name: "analytics_orders_in_current_window",
  help: "Number of orders in the current incomplete minute window",
});

// 2. Counter (ตัวนับ) สำหรับ Order ที่สำเร็จทั้งหมด (ค่าจะเพิ่มขึ้นเท่านั้น)
const ordersValidatedCounter = new client.Counter({
  name: "analytics_orders_validated_total",
  help: "Total number of validated orders processed by analytics-svc",
  labelNames: ["sku"], // เพิ่ม Label 'sku'
});

// 3. Gauge สำหรับนับยอดรวม SKU (ใช้แสดง Top 5 ใน Grafana ได้)
const skuTotalGauge = new client.Gauge({
  name: "analytics_sku_count_total",
  help: "Total count for each SKU processed",
  labelNames: ["sku"],
});

const app = express();
const getTopSkus = async () => {
  const skuCountsObject = await redisClient.hGetAll("sku_counts");

  if (!skuCountsObject || Object.keys(skuCountsObject).length === 0) {
    return []; // คืนค่าว่าง ถ้ายังไม่มีข้อมูล
  }

  //แปลง Object เป็น Array 
  const skuCountsArray = Object.entries(skuCountsObject).map(([sku, count]) => {
    return [sku, parseInt(count, 10)]; // แปลง "10" (string) เป็น 10 (number)
  });

  const sorted = skuCountsArray.sort((a, b) => b[1] - a[1]); // b[1] คือ count
  return sorted.slice(0, 5).map((item) => ({ sku: item[0], count: item[1] }));
};

app.get("/api/dashboard", async (req, res) => {
  const traceId = randomUUID();
  console.log(
    JSON.stringify({
      level: "INFO",
      traceId,
      service: SERVICE_NAME,
      component: "MetricsAPI",
      message: "JSON Metrics requested.",
    })
  );
  res.json({
    orders_in_current_minute_window: ordersInThisMinute,
    top_5_skus_all_time: await getTopSkus(),
  });
});

app.get("/metrics", async (req, res) => {
  try {
    res.set("Content-Type", client.register.contentType);
    res.end(await client.register.metrics());
  } catch (ex) {
    console.error(
      JSON.stringify({
        level: "ERROR",
        service: SERVICE_NAME,
        component: "MetricsAPI",
        message: "Error serving Prometheus metrics",
        error: ex.message,
      })
    );
    res.status(500).end(ex);
  }
});

const runConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "orders.validated", fromBeginning: false });
  console.log(
    JSON.stringify({
      level: "INFO",
      service: SERVICE_NAME,
      component: "Consumer",
      message: "Connected. Waiting for validated orders...",
    })
  );

  await consumer.run({
    eachMessage: async ({ message }) => {
      const order = await registry.decode(message.value);
      const { orderId, traceId, sku } = order;

      const idempotencyKey = `processed:analytics:${orderId}`;
      const isNew = await redisClient.set(idempotencyKey, "true", {
        EX: 86400, // 1 day
        NX: true,
      });
      if (!isNew) {
        console.warn(
          JSON.stringify({
            level: "WARN",
            traceId,
            orderId,
            service: SERVICE_NAME,
            message:
              "Idempotency check (Redis): Analytics already processed. Skipping.",
          })
        );
        return; // ข้ามการนับ (Kafka จะ auto-commit offset นี้ให้)
      }
      ordersInThisMinute++;

      ordersInWindowGauge.set(ordersInThisMinute);

      console.log(
        JSON.stringify({
          level: "DEBUG",
          traceId,
          orderId,
          service: SERVICE_NAME,
          component: "Consumer",
          message: "Order count in window incremented",
          ordersInThisMinute,
        })
      );

      const newSkuCount = await redisClient.hIncrBy("sku_counts", order.sku, 1);

      ordersValidatedCounter.inc({ sku: order.sku });
      skuTotalGauge.set({ sku: order.sku }, newSkuCount);

      console.log(
        JSON.stringify({
          level: "DEBUG",
          traceId,
          orderId,
          service: SERVICE_NAME,
          component: "Consumer",
          message: "Updated SKU count",
          sku: order.sku,
          newSkuCount: newSkuCount,
        })
      );
    },
  });
};

const runWindowTimer = async () => {
  await producer.connect();
  const { id: metricsSchemaId } = await registry.register(metricsSchema);

  setInterval(async () => {
    const windowTraceId = randomUUID();
    const timestamp = new Date().toISOString();
    const count = ordersInThisMinute;

    console.log(
      JSON.stringify({
        level: "INFO",
        traceId: windowTraceId,
        service: SERVICE_NAME,
        component: "WindowTimer",
        message: "--- 1 MINUTE WINDOW CLOSED ---",
        count,
      })
    );

    const dataToSend = { timestamp, count, traceId: windowTraceId };

    const payload = await registry.encode(metricsSchemaId, dataToSend);
    await producer.send({
      topic: "metrics.orders.per_minute",
      messages: [
        {
          key: timestamp,
          value: payload,
        },
      ],
    }); // Reset หน้าต่าง

    ordersInThisMinute = 0;

    ordersInWindowGauge.set(0);
  }, 60000); // 1 นาที
};

const main = async () => {
  try {
    await redisClient.connect();
    console.log(
      JSON.stringify({
        level: "INFO",
        service: SERVICE_NAME,
        message: "Redis connected.",
      })
    );
  } catch (err) {
    console.error(
      JSON.stringify({
        level: "FATAL",
        service: SERVICE_NAME,
        message: "Failed to connect to Redis. Exiting.",
        error: err.message,
      })
    );
    process.exit(1);
  }

  app.listen(4000, () => {
    console.log(
      JSON.stringify({
        level: "INFO",
        service: SERVICE_NAME,
        component: "MetricsAPI",
        message:
          "Listening on http://localhost:4000. Metrics at /metrics, JSON at /api/dashboard",
      })
    );
  });
  await Promise.all([runConsumer(), runWindowTimer()]);
};

main().catch((e) =>
  console.error(
    JSON.stringify({
      level: "FATAL",
      service: SERVICE_NAME,
      message: "[Analytics SVC] Error",
      error: e.message,
      stack: e.stack,
    })
  )
);

import { Kafka } from 'kafkajs';
import express from 'express';
import { SchemaRegistry, SchemaType } from '@kafkajs/confluent-schema-registry';
import { randomUUID } from 'crypto';

const SERVICE_NAME = 'analytics-svc';

// 2. เชื่อมต่อ Schema Registry
const registry = new SchemaRegistry({
  host: 'http://localhost:8081/apis/ccompat/v7',
});

// 3. (V1) กำหนด "พิมพ์เขียว" (Schema) สำหรับ *ขาออก*
const metricsSchema = {
  type: SchemaType.AVRO,
  schema: JSON.stringify({
    type: 'record',
    name: 'OrdersPerMinute',
    namespace: 'com.mycorp.metrics',
    fields: [
      { name: 'timestamp', type: 'string' },
      { name: 'count', type: 'int' },
      { name: 'traceId', type: ['null', 'string'], default: null }
    ],
  }),
};

// (In-Memory State...)
const skuCounts = new Map();
let ordersInThisMinute = 0;

// (Kafka Setup...)
const kafka = new Kafka({
  clientId: SERVICE_NAME,
  brokers: ['localhost:9092'],
});
const consumer = kafka.consumer({ groupId: 'analytics-processor-group' });
const producer = kafka.producer();

// (Express Server...)
const app = express();
const getTopSkus = () => {
  const sorted = [...skuCounts.entries()].sort((a, b) => b[1] - a[1]);
  return sorted.slice(0, 5).map(item => ({ sku: item[0], count: item[1] }));
};
app.get('/metrics', (req, res) => {
  // ---!!! 5. API ก็ควรมี traceId !!!---
  const traceId = randomUUID(); 
  console.log(JSON.stringify({
    level: "INFO",
    traceId,
    service: SERVICE_NAME,
    component: "MetricsAPI",
    message: "Metrics requested."
  }));
  res.json({
    orders_in_current_minute_window: ordersInThisMinute,
    top_5_skus_all_time: getTopSkus()
  });
});

// 4. STREAMING LOGIC (CONSUMER)
const runConsumer = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: 'orders.validated', fromBeginning: false });
  console.log(JSON.stringify({
    level: "INFO",
    service: SERVICE_NAME,
    component: "Consumer",
    message: "Connected. Waiting for validated orders..."
  }));

  await consumer.run({
    eachMessage: async ({ message }) => {
      
      const order = await registry.decode(message.value);
      
      // ---!!! 6. ดึง traceId และ orderId ออกมา !!!---
      const { orderId, traceId, sku } = order;
      
      ordersInThisMinute++;
      // ---!!! 7. เปลี่ยน Log เป็น Structured Log (ใช้ DEBUG เพราะมันจะเยอะมาก) ---!!!
      console.log(JSON.stringify({
        level: "DEBUG",
        traceId, // <-- ใช้ traceId ที่รับมา
        orderId,
        service: SERVICE_NAME,
        component: "Consumer",
        message: "Order count in window incremented",
        ordersInThisMinute,
      }));
      
      const currentSkuCount = skuCounts.get(order.sku) || 0;
      skuCounts.set(order.sku, currentSkuCount + 1);
      console.log(JSON.stringify({
        level: "DEBUG",
        traceId, // <-- ใช้ traceId ที่รับมา
        orderId,
        service: SERVICE_NAME,
        component: "Consumer",
        message: "Updated SKU count",
        sku: order.sku,
        newSkuCount: skuCounts.get(order.sku)
      }));
    },
  });
};

// 5. WINDOWING LOGIC (TIMER)
const runWindowTimer = async () => {
  await producer.connect();
  
  const { id: metricsSchemaId } = await registry.register(metricsSchema);

  setInterval(async () => {
    // ---!!! 8. สร้าง traceId ใหม่ สำหรับ Event นี้ (Metric Event) !!!---
    const windowTraceId = randomUUID(); 
    const timestamp = new Date().toISOString();
    const count = ordersInThisMinute;

    console.log(JSON.stringify({
      level: "INFO",
      traceId: windowTraceId, // <-- ใช้ traceId ใหม่
      service: SERVICE_NAME,
      component: "WindowTimer",
      message: "--- 1 MINUTE WINDOW CLOSED ---",
      count,
    }));

    const dataToSend = { timestamp, count, traceId: windowTraceId }; // <-- 9. เพิ่ม traceId

    const payload = await registry.encode(metricsSchemaId, dataToSend);
    await producer.send({
      topic: 'metrics.orders.per_minute',
      messages: [{
        key: timestamp,
        value: payload,
      }]
    });

    // Reset หน้าต่าง
    ordersInThisMinute = 0;
  }, 60000); // 1 นาที
};

// (Main Function...)
const main = async () => {
  app.listen(4000, () => {
    console.log(JSON.stringify({
      level: "INFO",
      service: SERVICE_NAME,
      component: "MetricsAPI",
      message: "Listening on http://localhost:4000/metrics",
    }));
  });
  await Promise.all([
    runConsumer(),
    runWindowTimer()
  ]);
};

main().catch(e => console.error(JSON.stringify({
  level: "FATAL",
  service: SERVICE_NAME,
  message: "[Analytics SVC] Error",
  error: e.message,
  stack: e.stack,
})));
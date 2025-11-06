import express from "express";
import { Kafka } from "kafkajs";
import { randomUUID } from "crypto"; // <-- 1. Import
import { SchemaRegistry, SchemaType } from "@kafkajs/confluent-schema-registry";

const SERVICE_NAME = "order-api";

// 2. เชื่อมต่อ Schema Registry
const registry = new SchemaRegistry({
  host: "http://localhost:8081/apis/ccompat/v7",
});

// --- Kafka Producer Setup ---
const kafka = new Kafka({
  clientId: SERVICE_NAME,
  brokers: ["localhost:9092"],
});
const producer = kafka.producer();

// 3. (V2) กำหนด "พิมพ์เขียว" (Schema)
const orderCreatedSchemaV2 = {
  type: SchemaType.AVRO,
  schema: JSON.stringify({
    type: "record",
    name: "OrderCreated",
    namespace: "com.mycorp.orders",
    fields: [
      { name: "orderId", type: "string" },
      { name: "sku", type: "string" },
      { name: "quantity", type: "int" },
      { name: "timestamp", type: "string" },
      {
        name: "customerEmail",
        type: ["null", "string"],
        default: null,
      },
      { name: 'traceId', type: ['null', 'string'], default: null }
    ],
  }),
};

// --- Express App Setup ---
const app = express();
app.use(express.json());

// --- Endpoint: POST /orders ---
app.post("/orders", async (req, res) => {
  // ---!!! 5. สร้าง traceId และตัวแปรสำหรับ Log !!!---
  const traceId = randomUUID();
  const service = SERVICE_NAME;

  const { sku, quantity } = req.body;
  if (!sku || !quantity) {
    // ---!!! 6. เปลี่ยน Log เป็น Structured Log !!!---
    console.warn(
      JSON.stringify({
        level: "WARN",
        traceId,
        service,
        message: "Bad request: SKU and quantity are required.",
      })
    );
    return res.status(400).json({ message: "SKU and quantity are required." });
  }

  const orderId = randomUUID();
  const order = {
    orderId: orderId,
    sku: sku,
    quantity: parseInt(quantity, 10),
    timestamp: new Date().toISOString(),
    customerEmail: "example@customer.com",
    traceId: traceId, 
  };

  try {
    const { id } = await registry.register(orderCreatedSchemaV2);
    const payload = await registry.encode(id, order);

    await producer.send({
      topic: "orders.created",
      messages: [{ key: orderId, value: payload }],
    });

    // ---!!! 6. เปลี่ยน Log เป็น Structured Log !!!---
    console.log(
      JSON.stringify({
        level: "INFO",
        traceId,
        orderId,
        service,
        message: "Order produced",
        schemaId: id,
      })
    );

    res.status(202).json({
      message: "Order received, processing...",
      orderId: orderId,
    });
  } catch (error) {
    // ---!!! 6. เปลี่ยน Log เป็น Structured Log !!!---
    console.error(
      JSON.stringify({
        level: "ERROR",
        traceId,
        orderId, // อาจจะยังไม่มี
        service,
        message: "Error producing order",
        error: error.message,
        stack: error.stack,
      })
    );
    res.status(500).json({ message: "Error processing order." });
  }
});

// --- Run Server ---
const run = async () => {
  await producer.connect();
  app.listen(3000, () => {
    // ---!!! 6. เปลี่ยน Log เป็น Structured Log !!!---
    console.log(
      JSON.stringify({
        level: "INFO",
        service: SERVICE_NAME,
        message: "Order API listening on port 3000",
      })
    );
  });
};

run().catch((e) =>
  console.error(
    JSON.stringify({
      level: "FATAL",
      service: SERVICE_NAME,
      message: "Failed to run server",
      error: e.message,
      stack: e.stack,
    })
  )
);
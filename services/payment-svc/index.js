import { Kafka } from "kafkajs";
import { SchemaRegistry, SchemaType } from "@kafkajs/confluent-schema-registry";
import { createClient } from "redis"; // <<< IMPORT REDIS

const SERVICE_NAME = "payment-svc";

// 2. เชื่อมต่อ Schema Registry
const registry = new SchemaRegistry({
  host: "http://localhost:8081/apis/ccompat/v7",
});

// 3. (V1) กำหนด "พิมพ์เขียว" (Schema) สำหรับ *ขาออก*
const paymentApprovedSchema = {
  type: SchemaType.AVRO,
  schema: JSON.stringify({
    type: "record",
    name: "PaymentApproved",
    namespace: "com.mycorp.payments",
    fields: [
      { name: "orderId", type: "string" },
      { name: "sku", type: "string" },
      { name: "quantity", type: "int" },
      { name: "timestamp", type: "string" },
      { name: "transactionId", type: "string" },
      { name: "traceId", type: ["null", "string"], default: null },
    ],
  }),
};

const paymentFailedSchema = {
  type: SchemaType.AVRO,
  schema: JSON.stringify({
    type: "record",
    name: "PaymentFailed",
    namespace: "com.mycorp.payments",
    fields: [
      { name: "orderId", type: "string" },
      { name: "sku", type: "string" },
      { name: "quantity", type: "int" },
      { name: "timestamp", type: "string" },
      { name: "reason", type: "string" },
      { name: "traceId", type: ["null", "string"], default: null },
    ],
  }),
};

// const processedOrderIds = new Set(); // <<< REMOVED
const kafka = new Kafka({
  clientId: SERVICE_NAME,
  brokers: ["localhost:9092"],
});

// <<< ADD: Setup Redis Client >>>
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
// <<< END ADD >>>

const consumer = kafka.consumer({
  groupId: "payment-processor-group",
  autoCommit: false,
});
const producer = kafka.producer();

const simulatePaymentApi = (orderId, traceId) => {
  const isSuccess = Math.random() < 0.8;
  if (isSuccess) {
    console.log(
      JSON.stringify({
        level: "INFO",
        traceId,
        orderId,
        service: SERVICE_NAME,
        component: "PaymentAPI",
        message: "Payment Approved.",
      })
    );
    return { status: "APPROVED", transactionId: `PAY-${Date.now()}` };
  } else {
    console.warn(
      JSON.stringify({
        level: "WARN",
        traceId,
        orderId,
        service: SERVICE_NAME,
        component: "PaymentAPI",
        message: "Payment Failed (e.g., Insufficient Funds).",
      })
    );
    throw new Error("Insufficient Funds");
  }
};

const run = async () => {
  const { id: approvedSchemaId } = await registry.register(
    paymentApprovedSchema
  );
  const { id: failedSchemaId } = await registry.register(paymentFailedSchema);

  // <<< ADD: Connect Redis >>>
  await redisClient.connect();
  console.log(
    JSON.stringify({
      level: "INFO",
      service: SERVICE_NAME,
      message: "Redis connected.",
    })
  );
  // <<< END ADD >>>

  await consumer.connect();
  await consumer.subscribe({ topic: "orders.validated", fromBeginning: false });
  await producer.connect();

  console.log(
    JSON.stringify({
      level: "INFO",
      service: SERVICE_NAME,
      message: "Connected. Waiting for validated orders...",
    })
  );

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = await registry.decode(message.value);
      const { orderId, traceId } = order;

      console.log(
        JSON.stringify({
          level: "INFO",
          traceId,
          orderId,
          partition,
          service: SERVICE_NAME,
          message: "Received Order",
        })
      );

      // <<< CHANGED: Idempotency Check (Redis) >>>
      const idempotencyKey = `processed:payment:${orderId}`;
      const isNew = await redisClient.set(idempotencyKey, "true", {
        EX: 86400, // Expire in 1 day
        NX: true, // Set only if Not eXists
      });

      if (!isNew) {
        // ซ้ำ
        console.warn(
          JSON.stringify({
            level: "WARN",
            traceId,
            orderId,
            service: SERVICE_NAME,
            message:
              "Idempotency check (Redis): Order already processed. Skipping.",
          })
        );
        await consumer.commitOffsets([
          { topic, partition, offset: message.offset },
        ]);
        return;
      }
      // <<< END CHANGED >>>

      try {
        const paymentResult = simulatePaymentApi(orderId, traceId);
        const successData = { ...order, ...paymentResult };
        const payload = await registry.encode(approvedSchemaId, successData);
        await producer.send({
          topic: "payments.approved",
          messages: [{ key: orderId, value: payload }],
        });
        console.log(
          JSON.stringify({
            level: "INFO",
            traceId,
            orderId,
            service: SERVICE_NAME,
            message: "Sent to payments.approved",
          })
        );

      } catch (error) {
        const failureData = { ...order, reason: error.message };
        const payload = await registry.encode(failedSchemaId, failureData);
        await producer.send({
          topic: "payments.failed",
          messages: [{ key: orderId, value: payload }],
        });
        console.log(
          JSON.stringify({
            level: "INFO",
            traceId,
            orderId,
            service: SERVICE_NAME,
            message: "Sent to payments.failed",
            reason: error.message,
          })
        );
      } finally {
        // processedOrderIds.add(orderId); // <<< REMOVED
        await consumer.commitOffsets([
          { topic, partition, offset: message.offset },
        ]);
      }
    },
  });
};

run().catch((e) =>
  console.error(
    JSON.stringify({
      level: "FATAL",
      service: SERVICE_NAME,
      message: "[Payment SVC] Error",
      error: e.message,
      stack: e.stack,
    })
  )
);

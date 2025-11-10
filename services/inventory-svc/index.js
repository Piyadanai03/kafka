import { Kafka } from "kafkajs";
import { SchemaRegistry, SchemaType } from "@kafkajs/confluent-schema-registry";
import { createClient } from "redis"; // <<< IMPORT REDIS

const SERVICE_NAME = "inventory-svc";

// 2. เชื่อมต่อ Schema Registry
const registry = new SchemaRegistry({
  host: "http://localhost:8081/apis/ccompat/v7",
});

// 3. (V1) กำหนด "พิมพ์เขียว" (Schema) สำหรับ *ขาออก*
const orderValidatedSchema = {
  type: SchemaType.AVRO,
  schema: JSON.stringify({
    type: "record",
    name: "OrderValidated",
    namespace: "com.mycorp.orders",
    fields: [
      { name: "orderId", type: "string" },
      { name: "sku", type: "string" },
      { name: "quantity", type: "int" },
      { name: "timestamp", type: "string" },
      { name: "customerEmail", type: ["null", "string"], default: null },
      { name: "traceId", type: ["null", "string"], default: null },
    ],
  }),
};

const orderRejectedSchema = {
  type: SchemaType.AVRO,
  schema: JSON.stringify({
    type: "record",
    name: "OrderRejected",
    namespace: "com.mycorp.orders",
    fields: [
      { name: "orderId", type: "string" },
      { name: "sku", type: "string" },
      { name: "quantity", type: "int" },
      { name: "timestamp", type: "string" },
      { name: "customerEmail", type: ["null", "string"], default: null },
      { name: "reason", type: "string" },
      { name: "traceId", type: ["null", "string"], default: null },
    ],
  }),
};

// (Global State, Kafka setup...)
const stock = new Map();
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

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
class OutOfStockError extends Error {
  constructor(message) {
    super(message);
    this.name = "OutOfStockError";
  }
}

const loadStock = async () => {
  const stockConsumer = kafka.consumer({
    groupId: `inventory-loader-group-${Date.now()}`,
  });
  await stockConsumer.connect();
  await stockConsumer.subscribe({
    topic: "inventory.stock",
    fromBeginning: true,
  });
  console.log(
    JSON.stringify({
      level: "INFO",
      service: SERVICE_NAME,
      component: "StockLoader",
      message: "Connected. Loading initial stock...",
    })
  );

  const partitionCount = 1;
  let partitionsDone = new Set();

  await new Promise((resolve, reject) => {
    stockConsumer
      .run({
        eachBatch: async ({
          batch,
          resolveOffset,
          heartbeat,
          isRunning,
          isStale,
        }) => {
          if (!isRunning() || isStale()) return;
          try {
            partitionsDone.add(batch.partition);
            for (const message of batch.messages) {
              if (!message.value) continue;
              const { sku, quantity } = JSON.parse(message.value.toString());
              stock.set(sku, quantity);
              console.log(
                JSON.stringify({
                  level: "DEBUG",
                  service: SERVICE_NAME,
                  component: "StockLoader",
                  message: `Loaded: ${sku} = ${quantity}`,
                })
              );
              resolveOffset(message.offset);
            }
            await heartbeat();
            if (partitionsDone.size === partitionCount) {
              console.log(
                JSON.stringify({
                  level: "INFO",
                  service: SERVICE_NAME,
                  component: "StockLoader",
                  message: "Finished loading all stock.",
                  stock: Object.fromEntries(stock),
                })
              );
              resolve();
            }
          } catch (e) {
            reject(e);
          }
        },
      })
      .catch(reject);
  });

  console.log(
    JSON.stringify({
      level: "INFO",
      service: SERVICE_NAME,
      component: "StockLoader",
      message: "Disconnecting stockConsumer...",
    })
  );
  await stockConsumer.disconnect();
  console.log(
    JSON.stringify({
      level: "INFO",
      service: SERVICE_NAME,
      component: "StockLoader",
      message: "Disconnected.",
    })
  );
};

const processOrders = async () => {
  const producer = kafka.producer();
  const orderConsumer = kafka.consumer({
    groupId: "inventory-processor-group",
    autoCommit: false,
  });

  const { id: validatedSchemaId } = await registry.register(
    orderValidatedSchema
  );
  const { id: rejectedSchemaId } = await registry.register(orderRejectedSchema);

  // Connect Redis
  await redisClient.connect();
  console.log(
    JSON.stringify({
      level: "INFO",
      service: SERVICE_NAME,
      message: "Redis connected.",
    })
  );

  await orderConsumer.connect();
  await orderConsumer.subscribe({
    topic: "orders.created"
  });
  await producer.connect();

  console.log(
    JSON.stringify({
      level: "INFO",
      service: SERVICE_NAME,
      component: "OrderProcessor",
      message: "Connected. Waiting for orders...",
    })
  );

  await orderConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (!message.value) {
        console.log(
          JSON.stringify({
            level: "INFO",
            service: SERVICE_NAME,
            message: "Received tombstone message, skipping.",
          })
        );
        // 2. Commit Offset ของ Message ที่เป็น null นี้ทิ้งไป
        await orderConsumer.commitOffsets([
          { topic, partition, offset: message.offset },
        ]);
        return; // 3. ข้ามไปทำงาน Message ถัดไป
      }
      await sleep(2000);
      const order = JSON.parse(message.value.toString());
      const { orderId, sku, quantity, traceId } = order;

      console.log(
        JSON.stringify({
          level: "INFO",
          traceId,
          orderId,
          service: SERVICE_NAME,
          component: "OrderProcessor",
          message: "Received Order",
          sku,
          quantity,
        })
      );

      // <<< CHANGED: Idempotency Check (Redis) >>>
      const idempotencyKey = `processed:inventory:${orderId}`;
      const isNew = await redisClient.set(idempotencyKey, "true", {
        EX: 86400, // 1 day expiry
        NX: true, // Set only if Not eXists
      });

      if (!isNew) {
        // ซ้ำ
        console.warn(
          JSON.stringify({
            level: "WARN",
            traceId,
            orderId,
            message:
              "Idempotency check (Redis): Order already processed. Skipping.",
          })
        );
        await orderConsumer.commitOffsets([
          { topic, partition, offset: message.offset },
        ]);
        return;
      }
      const MAX_RETRIES = 3;
      for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
          const currentStock = stock.get(sku) || 0;
          if (currentStock < quantity) {
            throw new OutOfStockError(
              `Not enough stock for ${sku}: ${currentStock} < ${quantity}`
            );
          }
          if (Math.random() < 0.1 && attempt < MAX_RETRIES) {
            throw new Error("Simulated transient database error!");
          }

          stock.set(sku, currentStock - quantity);
          console.log(
            JSON.stringify({
              level: "INFO",
              traceId,
              orderId,
              service: SERVICE_NAME,
              component: "OrderProcessor",
              message: "[Success] Stock updated",
              sku,
              newStock: stock.get(sku),
            })
          );
          const successPayload = await registry.encode(
            validatedSchemaId,
            order
          );
          await producer.send({
            topic: "orders.validated",
            messages: [{ key: orderId, value: successPayload }],
          });

          await orderConsumer.commitOffsets([
            { topic, partition, offset: message.offset },
          ]);
          return;
        } catch (error) {
          if (error instanceof OutOfStockError) {
            console.error(
              JSON.stringify({
                level: "ERROR" /* ... */,
                message: "[Business Failure] Out of stock",
                error: error.message,
              })
            );

            const rejectionData = { ...order, reason: error.message };
            const failurePayload = await registry.encode(
              rejectedSchemaId,
              rejectionData
            );
            await producer.send({
              topic: "orders.rejected",
              messages: [{ key: orderId, value: failurePayload }],
            });

            // processedOrderIds.add(orderId); // <<< REMOVED
            await orderConsumer.commitOffsets([
              { topic, partition, offset: message.offset },
            ]);
            return;
          }

          console.warn(
            JSON.stringify({
              /* ... */
              message: `[Retryable Failure] Attempt ${attempt}/${MAX_RETRIES}`,
              error: error.message,
            })
          );

          if (attempt === MAX_RETRIES) {
            console.error(
              JSON.stringify({
                /* ... */ message: "[DLQ] Max retries reached. Sending to DLQ.",
                error: error.message,
              })
            ); // ... producer.send to DLQ ...
            // processedOrderIds.add(orderId); // <<< REMOVED
            await orderConsumer.commitOffsets([
              { topic, partition, offset: message.offset },
            ]);
            return;
          }

          const backoffTime = 50 * attempt;
          await sleep(backoffTime);
        }
      }
    },
  });
};

// --- Main Function ---
const main = async () => {
  try {
    await loadStock();
  } catch (e) {
    console.error(
      JSON.stringify({
        level: "FATAL",
        service: SERVICE_NAME,
        component: "StockLoader",
        message: "Failed to load initial stock. Exiting.",
        error: e.message,
        stack: e.stack,
      })
    );
    process.exit(1);
  }

  console.log(
    JSON.stringify({
      level: "INFO",
      service: SERVICE_NAME,
      component: "StockLoader",
      message: "Initial stock loaded successfully.",
      stock: Object.fromEntries(stock),
    })
  );

  await processOrders();
};

main().catch((e) => {
  console.error(
    JSON.stringify({
      level: "FATAL",
      service: SERVICE_NAME,
      message: "---!!! MAIN CATCH BLOCK !!!---",
      error: e.message,
      stack: e.stack,
    })
  );
});

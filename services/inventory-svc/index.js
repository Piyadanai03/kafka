import { Kafka } from "kafkajs";
import { SchemaRegistry, SchemaType } from "@kafkajs/confluent-schema-registry";

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
      // (ควรสืบทอด field อื่นๆ มาด้วย)
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

// (Global State, Kafka setup, sleep, OutOfStockError... )
const stock = new Map();
const processedOrderIds = new Set();
const kafka = new Kafka({
  clientId: SERVICE_NAME, // <-- 1. ใช้ชื่อ Service
  brokers: ["localhost:9092"],
});
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
class OutOfStockError extends Error {
  constructor(message) {
    super(message);
    this.name = "OutOfStockError";
  }
}

const loadStock = async () => {
  const stockConsumer = kafka.consumer({ 
    groupId: `inventory-loader-group-${Date.now()}` 
  });
  await stockConsumer.connect();
  await stockConsumer.subscribe({
    topic: 'inventory.stock',
    fromBeginning: true,
  });
  
  console.log(JSON.stringify({
    level: "INFO",
    service: SERVICE_NAME,
    component: "StockLoader",
    message: "Connected. Loading initial stock...",
  }));

  const partitionCount = 1; 
  let partitionsDone = new Set();

  await new Promise((resolve, reject) => {
    stockConsumer.run({
      eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
        if (!isRunning() || isStale()) return;
        try {
          // เพิ่ม partition เข้า Set
          partitionsDone.add(batch.partition);
          
          for (const message of batch.messages) {
            if (!message.value) continue;
            const { sku, quantity } = JSON.parse(message.value.toString());
            stock.set(sku, quantity);
            
            console.log(JSON.stringify({
                level: "DEBUG",
                service: SERVICE_NAME,
                component: "StockLoader",
                message: `Loaded: ${sku} = ${quantity}`
            }));
            resolveOffset(message.offset);
          }
          await heartbeat();
          
          // เช็คว่าอ่านครบทุก partition แล้ว
          if (partitionsDone.size === partitionCount) {
            console.log(JSON.stringify({
                level: "INFO",
                service: SERVICE_NAME,
                component: "StockLoader",
                message: "Finished loading all stock.",
                stock: Object.fromEntries(stock)
            }));
            resolve();
          }
        } catch (e) {
          reject(e);
        }
      },
    }).catch(reject);
  });

  console.log(JSON.stringify({ 
    level: "INFO", 
    service: SERVICE_NAME, 
    component: "StockLoader", 
    message: "Disconnecting stockConsumer..."
  }));
  await stockConsumer.disconnect();
  console.log(JSON.stringify({ 
    level: "INFO", 
    service: SERVICE_NAME, 
    component: "StockLoader", 
    message: "Disconnected."
  }));
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

  await orderConsumer.connect();
  await orderConsumer.subscribe({ topic: "orders.created" });
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
      await sleep(2000);
      const order = await registry.decode(message.value);

      // ---!!! 6. ดึง traceId และ orderId ออกมา !!!---
      const { orderId, sku, quantity, traceId } = order;

      console.log(
        JSON.stringify({
          level: "INFO",
          traceId, // <-- ล็อก
          orderId, // <-- ล็อก
          service: SERVICE_NAME,
          component: "OrderProcessor",
          message: "Received Order",
          sku,
          quantity,
        })
      );

      if (processedOrderIds.has(orderId)) {
        console.warn(
          JSON.stringify({
            level: "WARN",
            traceId,
            orderId,
            service: SERVICE_NAME,
            component: "OrderProcessor",
            message: "Idempotency check: Order already processed. Skipping.",
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

          // ---!!! 7. ส่งต่อ order object ที่มี traceId อยู่แล้ว ---!!!
          // (Object `order` ที่ decode มา มี traceId อยู่แล้ว)
          const successPayload = await registry.encode(
            validatedSchemaId,
            order
          );
          await producer.send({
            topic: "orders.validated",
            messages: [{ key: orderId, value: successPayload }],
          });

          processedOrderIds.add(orderId);
          await orderConsumer.commitOffsets([
            { topic, partition, offset: message.offset },
          ]);
          return;
        } catch (error) {
          if (error instanceof OutOfStockError) {
            console.error(
              JSON.stringify({
                level: "ERROR",
                traceId,
                orderId,
                service: SERVICE_NAME,
                component: "OrderProcessor",
                message: "[Business Failure] Out of stock",
                error: error.message,
              })
            );

            // ---!!! 7. ส่งต่อ order object + reason ---!!!
            const rejectionData = { ...order, reason: error.message };
            const failurePayload = await registry.encode(
              rejectedSchemaId,
              rejectionData
            );
            await producer.send({
              topic: "orders.rejected",
              messages: [{ key: orderId, value: failurePayload }],
            });

            processedOrderIds.add(orderId);
            await orderConsumer.commitOffsets([
              { topic, partition, offset: message.offset },
            ]);
            return;
          }

          console.warn(
            JSON.stringify({
              level: "WARN",
              traceId,
              orderId,
              service: SERVICE_NAME,
              component: "OrderProcessor",
              message: `[Retryable Failure] Attempt ${attempt}/${MAX_RETRIES}`,
              error: error.message,
            })
          );

          if (attempt === MAX_RETRIES) {
            console.error(
              JSON.stringify({
                level: "ERROR",
                traceId,
                orderId,
                service: SERVICE_NAME,
                component: "OrderProcessor",
                message: "[DLQ] Max retries reached. Sending to DLQ.",
                error: error.message,
              })
            );
            // ... producer.send to DLQ ...
            processedOrderIds.add(orderId);
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

  // 3. เริ่มประมวลผล Order
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

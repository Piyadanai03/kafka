// services/inventory-svc/index.js
import { Kafka } from 'kafkajs';

// --- Global State (In-Memory DB) ---
const stock = new Map(); // Map<sku, quantity>
const processedOrderIds = new Set(); // Set<orderId>
// ------------------------------------

const kafka = new Kafka({
  clientId: 'inventory-svc',
  brokers: ['localhost:9092'],
});

// --- Utility: Exponential Backoff ---
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
class OutOfStockError extends Error {
  constructor(message) { super(message); this.name = 'OutOfStockError'; }
}

/**
 * ---------------------------------------------------
 * ขั้นตอนที่ 1: Bootstrap Stock (โหลดสต็อกเข้าระบบ)
 * (FIXED: แก้ไข Race Condition)
 * ---------------------------------------------------
 */
const loadStock = async () => {
  const stockConsumer = kafka.consumer({ 
    groupId: `inventory-loader-group-${Date.now()}` 
  });
  await stockConsumer.connect(); // <--- 1. Connect ก่อน
  await stockConsumer.subscribe({
    topic: 'inventory.stock',
    fromBeginning: true,
  });
  console.log('[Stock Loader] Connected. Loading initial stock...');

  const partitionCount = 1; 
  let partitionsDone = new Set();

  // <--- 2. สร้าง Promise ให้ .run() ทำงาน
  await new Promise((resolve, reject) => {
    stockConsumer.run({
      eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
        // (ส่วนนี้เหมือนเดิม)
        if (!isRunning() || isStale()) return;
        try {
          for (const message of batch.messages) {
            if (!message.value) continue;
            const { sku, quantity } = JSON.parse(message.value.toString());
            stock.set(sku, quantity);
            console.log(`[Stock Loader] Loaded: ${sku} = ${quantity}`);
            resolveOffset(message.offset);
          }
          await heartbeat();
          const lastOffsetInBatch = Number(batch.messages[batch.messages.length - 1].offset);
          const highWatermark = Number(batch.highWatermark);
          if (lastOffsetInBatch + 1 === highWatermark) {
            if (!partitionsDone.has(batch.partition)) {
              partitionsDone.add(batch.partition);
              console.log(`[Stock Loader] Finished reading partition ${batch.partition}`);
            }
          }
          if (partitionsDone.size === partitionCount) {
            console.log('[Stock Loader] Finished loading all stock.');
            console.log('Current Stock:', stock);
            resolve(); // <--- 3. เมื่ออ่านจบ ให้ Resolve Promise
          }
        } catch (e) {
          reject(e);
        }
      },
    }).catch(reject);
  });

  // <--- 4. (FIX) เมื่อ Promise จบแล้ว ค่อย Disconnect
  // สิ่งนี้จะเกิดขึ้น "ก่อน" ที่ main จะไปเรียก processOrders()
  console.log('[Stock Loader] Disconnecting stockConsumer...');
  await stockConsumer.disconnect();
  console.log('[Stock Loader] Disconnected.');
};


/**
 * ---------------------------------------------------
 * ขั้นตอนที่ 2: Process Orders (Consumer หลัก)
 * ---------------------------------------------------
 */
const processOrders = async () => {
  const producer = kafka.producer(); 
  const orderConsumer = kafka.consumer({ 
    groupId: 'inventory-processor-group',
    autoCommit: false,
  });
  
  await orderConsumer.connect();
  await orderConsumer.subscribe({ topic: 'orders.created' });
  await producer.connect();
  
  console.log('[Order Processor] Connected. Waiting for orders...');

  await orderConsumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());
      const { orderId, sku, quantity } = order;

      console.log(`\n[Order Processor] Received Order: ${orderId} (SKU: ${sku}, Qty: ${quantity})`);

      // 1. Idempotency Check
      if (processedOrderIds.has(orderId)) {
        console.warn(`[Idempotency] Order ${orderId} already processed. Skipping.`);
        await orderConsumer.commitOffsets([{ topic, partition, offset: message.offset }]);
        return;
      }

      // 2. Retry + DLQ Logic
      const MAX_RETRIES = 3;
      for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        try {
          // --- Business Logic ---
          const currentStock = stock.get(sku) || 0;
          if (currentStock < quantity) {
            throw new OutOfStockError(`Not enough stock for ${sku}: ${currentStock} < ${quantity}`);
          }
          
          if (Math.random() < 0.1 && attempt < MAX_RETRIES) {
             throw new Error('Simulated transient database error!');
          }

          // --- Success Path ---
          stock.set(sku, currentStock - quantity); // หักสต็อก
          console.log(`[Success] Stock updated for ${sku}: ${stock.get(sku)}`);

          await producer.send({
            topic: 'orders.validated',
            messages: [{ key: orderId, value: JSON.stringify(order) }]
          });

          processedOrderIds.add(orderId);
          await orderConsumer.commitOffsets([{ topic, partition, offset: message.offset }]);
          return;

        } catch (error) {
          
          // --- Business Failure (ของหมด) ---
          if (error instanceof OutOfStockError) {
            console.error(`[Business Failure] ${error.message}`);
            await producer.send({
              topic: 'orders.rejected',
              messages: [{ key: orderId, value: JSON.stringify({ ...order, reason: error.message }) }]
            });
            processedOrderIds.add(orderId);
            await orderConsumer.commitOffsets([{ topic, partition, offset: message.offset }]);
            return;
          }

          // --- Transient Failure (ระบบล่มชั่วคราว) ---
          console.warn(`[Retryable Failure] Attempt ${attempt}/${MAX_RETRIES} for order ${orderId}. Error: ${error.message}`);
          
          if (attempt === MAX_RETRIES) {
            // --- DLQ Path ---
            console.error(`[DLQ] All retries failed for order ${orderId}. Sending to DLQ.`);
            await producer.send({
              topic: 'inventory.dlq',
              messages: [{ 
                key: orderId, 
                value: JSON.stringify({
                  originalMessage: order,
                  error: error.message,
                  timestamp: new Date().toISOString()
                })
              }]
            });
            await orderConsumer.commitOffsets([{ topic, partition, offset: message.offset }]);
            return;
          }

          const backoffTime = (2 ** attempt) * 100;
          await sleep(backoffTime);
        }
      }
    },
  });
};

// --- Main Function ---
const main = async () => {
  await loadStock();
  await processOrders();
};

main().catch(e => {
    console.error("---!!! MAIN CATCH BLOCK !!!---");
    console.error(e);
});
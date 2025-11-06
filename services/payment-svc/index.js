import { Kafka } from 'kafkajs';
import { SchemaRegistry, SchemaType } from '@kafkajs/confluent-schema-registry';

const SERVICE_NAME = 'payment-svc'; // <-- 1. กำหนดชื่อ

// 2. เชื่อมต่อ Schema Registry
const registry = new SchemaRegistry({
  host: 'http://localhost:8081/apis/ccompat/v7',
});

// 3. (V1) กำหนด "พิมพ์เขียว" (Schema) สำหรับ *ขาออก*
const paymentApprovedSchema = {
  type: SchemaType.AVRO,
  schema: JSON.stringify({
    type: 'record',
    name: 'PaymentApproved',
    namespace: 'com.mycorp.payments',
    fields: [
      { name: 'orderId', type: 'string' },
      { name: 'sku', type: 'string' },
      { name: 'quantity', type: 'int' },
      { name: 'timestamp', type: 'string' },
      { name: 'transactionId', type: 'string' },
      { name: 'traceId', type: ['null', 'string'], default: null }
    ],
  }),
};

const paymentFailedSchema = {
  type: SchemaType.AVRO,
  schema: JSON.stringify({
    type: 'record',
    name: 'PaymentFailed',
    namespace: 'com.mycorp.payments',
    fields: [
      { name: 'orderId', type: 'string' },
      { name: 'sku', type: 'string' },
      { name: 'quantity', type: 'int' },
      { name: 'timestamp', type: 'string' },
      { name: 'reason', type: 'string' },
      { name: 'traceId', type: ['null', 'string'], default: null }
    ],
  }),
};

// (Idempotency Set, Kafka Setup...)
const processedOrderIds = new Set();
const kafka = new Kafka({
  clientId: SERVICE_NAME, // <-- 1. ใช้ชื่อ
  brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({
  groupId: 'payment-processor-group',
  autoCommit: false,
});
const producer = kafka.producer();

// (simulatePaymentApi...)
const simulatePaymentApi = (orderId, traceId) => { // <-- 5. รับ traceId
  const isSuccess = Math.random() < 0.8;
  if (isSuccess) {
    // ---!!! 6. เปลี่ยน Log เป็น Structured Log !!!---
    console.log(JSON.stringify({
      level: "INFO",
      traceId,
      orderId,
      service: SERVICE_NAME,
      component: "PaymentAPI",
      message: "Payment Approved.",
    }));
    return { status: 'APPROVED', transactionId: `PAY-${Date.now()}` };
  } else {
    console.warn(JSON.stringify({
      level: "WARN",
      traceId,
      orderId,
      service: SERVICE_NAME,
      component: "PaymentAPI",
      message: "Payment Failed (e.g., Insufficient Funds).",
    }));
    throw new Error('Insufficient Funds');
  }
};

const run = async () => {
  const { id: approvedSchemaId } = await registry.register(paymentApprovedSchema);
  const { id: failedSchemaId } = await registry.register(paymentFailedSchema);

  await consumer.connect();
  await consumer.subscribe({ topic: 'orders.validated', fromBeginning: false });
  await producer.connect();

  console.log(JSON.stringify({
    level: "INFO",
    service: SERVICE_NAME,
    message: "Connected. Waiting for validated orders...",
  }));

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      
      const order = await registry.decode(message.value);
      
      // ---!!! 7. ดึง traceId และ orderId ออกมา !!!---
      const { orderId, traceId } = order;

      console.log(JSON.stringify({
        level: "INFO",
        traceId,
        orderId,
        partition,
        service: SERVICE_NAME,
        message: "Received Order",
      }));

      if (processedOrderIds.has(orderId)) {
        console.warn(JSON.stringify({
          level: "WARN",
          traceId,
          orderId,
          service: SERVICE_NAME,
          message: "Idempotency check: Order already processed. Skipping.",
        }));
        await consumer.commitOffsets([{ topic, partition, offset: message.offset }]);
        return;
      }

      try {
        const paymentResult = simulatePaymentApi(orderId, traceId); // <-- 5. ส่ง traceId
        
        // ---!!! 8. ส่งต่อ ...order ที่มี traceId อยู่แล้ว ---!!!
        const successData = { ...order, ...paymentResult }; 
        const payload = await registry.encode(approvedSchemaId, successData);
        
        await producer.send({
          topic: 'payments.approved',
          messages: [{ key: orderId, value: payload }],
        });
        console.log(JSON.stringify({
          level: "INFO",
          traceId,
          orderId,
          service: SERVICE_NAME,
          message: "Sent to payments.approved",
        }));

      } catch (error) {
        // ---!!! 8. ส่งต่อ ...order ที่มี traceId อยู่แล้ว ---!!!
        const failureData = { ...order, reason: error.message }; 
        const payload = await registry.encode(failedSchemaId, failureData);
        
        await producer.send({
          topic: 'payments.failed',
          messages: [{ key: orderId, value: payload }],
        });
        console.log(JSON.stringify({
          level: "INFO",
          traceId,
          orderId,
          service: SERVICE_NAME,
          message: "Sent to payments.failed",
          reason: error.message,
        }));
      
      } finally {
        processedOrderIds.add(orderId);
        await consumer.commitOffsets([{ topic, partition, offset: message.offset }]);
      }
    },
  });
};

run().catch(e => console.error(JSON.stringify({
  level: "FATAL",
  service: SERVICE_NAME,
  message: "[Payment SVC] Error",
  error: e.message,
  stack: e.stack,
})));
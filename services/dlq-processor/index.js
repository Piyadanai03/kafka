import { Kafka } from "kafkajs";
import { createClient } from "redis";

const SERVICE_NAME = "dlq-processor-svc";

// Setup Kafka and Redis
const kafka = new Kafka({
  clientId: SERVICE_NAME,
  brokers: ["localhost:9092"],
});

const redisClient = createClient({
  url: "redis://localhost:6379",
});
redisClient.on("error", (err) => console.error("Redis Client Error", err));

const consumer = kafka.consumer({
  groupId: "dlq-processor-group",
  // จะ Commit offset เอง (Manual Commit)
  autoCommit: false,
});
const producer = kafka.producer();

const log = (level, message, context = {}) => {
  console.log(
    JSON.stringify({ level, service: SERVICE_NAME, message, ...context })
  );
};

const run = async () => {
  await consumer.connect();
  await producer.connect();
  await redisClient.connect();

  // ฟัง (Subscribe) ที่ 'inventory.dlq'
  await consumer.subscribe({
    topic: "inventory.dlq",
    fromBeginning: true,
  });

  log("INFO", "Connected. Waiting for failed messages from inventory.dlq...");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const { offset } = message;
      log("INFO", `Received DLQ Message at offset ${offset}`);

      // Parse DLQ Message
      const dlqMessage = JSON.parse(message.value.toString());
      const { originalTopic, originalPayloadString, error } = dlqMessage;

      // parse payload เดิมอีกชั้นหนึ่งเพื่อเอา key
      const originalOrder = JSON.parse(originalPayloadString);
      const { orderId } = originalOrder;

      // ป้องกันการประมวลผล DLQ Message เดิมซ้ำซ้อน
      const idempotencyKey = `processed:dlq:${orderId}:${offset}`;
      const isNew = await redisClient.set(idempotencyKey, "true", {
        EX: 86400, // 1 day
        NX: true, // Set only if Not eXists
      });

      if (!isNew) {
        log(
          "WARN",
          "Idempotency check: DLQ Message already processed. Skipping.",
          { orderId, offset }
        );
        // แม้จะข้ามไป แต่ก็ต้อง Commit offset นี้ทิ้งด้วย
        await consumer.commitOffsets([
          { topic, partition, offset: message.offset },
        ]);
        return;
      }

      // Logic การซ่อมแซม
      // ในเคสนี้ (Simulated transient error) การซ่อม คือการลองใหม่อีกครั้ง
      // ะส่ง Message "เดิม" กลับไปที่ Topic เดิม
      try {
        log(
          "INFO",
          "Attempting to re-process by sending back to original topic...",
          { orderId, originalTopic }
        );

        await producer.send({
          topic: originalTopic, // --> orders.created
          messages: [
            {
              key: orderId,
              value: originalPayloadString, // ส่ง Message เดิมกลับเข้าไป
            },
          ],
        });

        log("INFO", "[Success] Message re-queued to original topic.", {
          orderId,
          originalTopic,
        });

        // Commit Offset เมื่อสำเร็จ
        await consumer.commitOffsets([
          { topic, partition, offset: message.offset },
        ]);
      } catch (e) {
        // ถ้าล้มเหลวอีก (เช่น Kafka ล่ม)
        // จะไม่ Commit offset (เพื่อให้ Consumer ตัวอื่นลองใหม่)
        // แต่ถ้าล้มเหลวเพราะ Messageจริงๆจะส่งไป Parking Lot
        log("ERROR", "Failed to re-queue message. Sending to Parking Lot.", {
          orderId,
          error: e.message,
        });

        const parkingLotPayload = {
          ...dlqMessage,
          dlqProcessorError: e.message,
          dlqProcessorTimestamp: new Date().toISOString(),
        };

        await producer.send({
          topic: "parking-lot.inventory",
          messages: [
            { key: orderId, value: JSON.stringify(parkingLotPayload) },
          ],
        });

        // เมื่อส่งไป Parking Lot แล้ว ถือว่าจัดการแล้ว -> Commit Offset
        await consumer.commitOffsets([
          { topic, partition, offset: message.offset },
        ]);
      }
    },
  });
};

run().catch((e) =>
  log("FATAL", "DLQ Processor Error", { error: e.message, stack: e.stack })
);

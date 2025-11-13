import { Kafka } from "kafkajs";
const kafka = new Kafka({ brokers: ["localhost:9092"] });
const admin = kafka.admin();

const run = async () => {
  await admin.connect();
  await admin.createTopics({
    topics: [
      // Topic นี้สำหรับ Message ที่ DLQ Processor ก็ยังแก้ไม่ได้
      // (ไว้ให้คนมาดู Manual)
      { topic: "parking-lot.inventory", numPartitions: 1 },
    ],
  });
  console.log("Parking Lot topics created!");
  await admin.disconnect();
};
run().catch(console.error);

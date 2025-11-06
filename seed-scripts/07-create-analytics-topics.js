import { Kafka } from 'kafkajs';
const kafka = new Kafka({ brokers: ['localhost:9092'] });
const admin = kafka.admin();

const run = async () => {
  await admin.connect();
  await admin.createTopics({
    topics: [
      // Topic นี้จะเก็บข้อมูล เช่น { timestamp: "...", count: 120 }
      { topic: 'metrics.orders.per_minute', numPartitions: 1 },
    ],
  });
  console.log('Analytics topics created!');
  await admin.disconnect();
};
run().catch(console.error);
import { Kafka } from 'kafkajs';
const kafka = new Kafka({ brokers: ['localhost:9092'] });
const admin = kafka.admin();

const run = async () => {
  await admin.connect();
  await admin.createTopics({
    topics: [
      { topic: 'payments.approved', numPartitions: 3 },
      { topic: 'payments.failed', numPartitions: 3 },
      { topic: 'payments.dlq', numPartitions: 3 },
    ],
  });
  console.log('Payment topics created!');
  await admin.disconnect();
};
run().catch(console.error);
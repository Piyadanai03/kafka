import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'admin-client',
  brokers: ['localhost:9092'], 
});

const admin = kafka.admin();

const run = async () => {
  await admin.connect();
  console.log('Admin connected...');

  await admin.createTopics({
    validateOnly: false,
    topics: [
      {
        topic: 'orders.created',
        numPartitions: 3, //กำหนด 3 Partitions
        replicationFactor: 1, // ใน dev ใช้ 1 
      },
    ],
  });

  console.log('Topic "orders.created" created with 3 partitions!');

  // ลอง list topics ออกมาดู
  const topics = await admin.listTopics();
  console.log('Current topics:', topics);
  
  await admin.disconnect();
};

run().catch(console.error);
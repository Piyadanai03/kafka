import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'stock-seeder',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

// SKUs จากด่าน A
const skus = [
  'SKU-TSHIRT-RED', 'SKU-TSHIRT-BLK', 'SKU-MUG-LOGO', 'SKU-STICKER-SET',
  'SKU-HOODIE-BLK', 'SKU-HOODIE-WHT', 'SKU-CAP-BLK', 'SKU-BEANIE',
  'SKU-SOCKS-PAIR', 'SKU-POSTER-LARGE'
];

const run = async () => {
  await producer.connect();
  console.log('Producer connected...');

  // ส่งสต็อกเริ่มต้น: SKU ละ 10 ชิ้น
  const messages = skus.map(sku => ({
    key: sku, // Key คือ SKU
    value: JSON.stringify({ sku: sku, quantity: 100 }) // Value คือข้อมูลสต็อก
  }));

  await producer.send({
    topic: 'inventory.stock',
    messages: messages,
  });

  console.log('Initial stock sent to "inventory.stock" topic!');
  await producer.disconnect();
};

run().catch(console.error);
import express from "express";
import { Sequelize, DataTypes, Model } from "sequelize";
import { randomUUID } from "crypto";

const SERVICE_NAME = "order-api";

// ---!!! 1. Setup Sequelize (เชื่อมต่อ DB) !!!---
const sequelize = new Sequelize(
  "order_db", // Database name
  "postgres", // User
  "postgres", // Password
  {
    host: "localhost", // หรือ 'db' ถ้า service นี้รันใน Docker
    port: 5432,
    dialect: "postgres",
    logging: (msg) =>
      console.log(
        JSON.stringify({
          level: "DEBUG",
          service: SERVICE_NAME,
          component: "Sequelize",
          message: msg,
        })
      ),
  }
);

// ---!!! 2. Define Models (ต้องตรงกับตารางที่คุณสร้าง) !!!---

class Order extends Model {}
Order.init(
  {
    id: {
      type: DataTypes.UUID,
      primaryKey: true,
    },
    sku: {
      type: DataTypes.STRING(100),
      allowNull: false,
    },
    quantity: {
      type: DataTypes.INTEGER,
      allowNull: false,
    },
    customer_email: {
      type: DataTypes.STRING(255),
    },
  },
  {
    sequelize,
    modelName: "Order",
    tableName: "orders", // บังคับให้ตรงกับชื่อตาราง
    timestamps: true, // ใช้ created_at
    updatedAt: false,
    underscored: true,
  }
);

class OutboxEvent extends Model {}
OutboxEvent.init(
  {
    id: {
      type: DataTypes.UUID,
      primaryKey: true,
    },
    topic: {
      type: DataTypes.STRING(255),
      allowNull: false,
    },
    message_key: {
      type: DataTypes.STRING(255),
    },
    payload: {
      type: DataTypes.JSONB, // <--- สำคัญ
      allowNull: false,
    },
  },
  {
    sequelize,
    modelName: "OutboxEvent",
    tableName: "outbox_events", // บังคับให้ตรงกับชื่อตาราง
    timestamps: true, // ใช้ created_at
    updatedAt: false,
    underscored: true,
  }
);

// ---!!! 3. ลบ Kafka Producer ทั้งหมด !!!---
// (ไม่มี Kafka, ไม่มี Schema Registry ใน Service นี้อีกต่อไป)

// --- Express App Setup ---
const app = express();
app.use(express.json());

// --- Endpoint: POST /orders ---
app.post("/orders", async (req, res) => {
  const traceId = randomUUID();
  const service = SERVICE_NAME;

  const { sku, quantity } = req.body;
  if (!sku || !quantity) {
    console.warn(
      JSON.stringify({
        level: "WARN",
        traceId,
        service,
        message: "Bad request: SKU and quantity are required.",
      })
    );
    return res.status(400).json({ message: "SKU and quantity are required." });
  }

  const orderId = randomUUID();
  const orderData = {
    orderId: orderId,
    sku: sku,
    quantity: parseInt(quantity, 10),
    timestamp: new Date().toISOString(),
    customerEmail: "example@customer.com",
    traceId: traceId,
  };

  // ---!!! 4. ใช้ Database Transaction !!!---
  try {
    // 4.1. เริ่ม Transaction
    await sequelize.transaction(async (t) => {
      // 4.2. สร้าง Order (INSERT ลงตาราง 'orders')
      await Order.create(
        {
          id: orderData.orderId,
          sku: orderData.sku,
          quantity: orderData.quantity,
          customer_email: orderData.customerEmail,
        },
        { transaction: t }
      );

      // 4.3. สร้าง Event (INSERT ลงตาราง 'outbox_events')
      await OutboxEvent.create(
        {
          id: randomUUID(),
          topic: "orders.created", // <--- Topic ที่ Debezium จะอ่าน
          message_key: orderData.orderId, // <--- Key ของ Message
          payload: orderData, // <--- Payload ทั้งหมดของ Message
        },
        { transaction: t }
      );

      // 4.4. ถ้าถึงบรรทัดนี้ได้ -> Sequelize จะ Commit ให้อัตโนมัติ
    });

    // 4.5. Transaction สำเร็จ! (Commit เรียบร้อย)
    console.log(
      JSON.stringify({
        level: "INFO",
        traceId,
        orderId,
        service,
        message: "Order saved to DB and Outbox event created.",
      })
    );

    // ตอบกลับ Client ทันที
    res.status(202).json({
      message: "Order received, processing...",
      orderId: orderId,
    });
  } catch (error) {
    // 4.6. ถ้าเกิด Error -> Sequelize จะ Rollback ให้อัตโนมัติ
    // (ทั้ง 'orders' และ 'outbox_events' จะถูกย้อนกลับ)
    console.error(
      JSON.stringify({
        level: "ERROR",
        traceId,
        orderId,
        service,
        message: "Error processing order (Transaction Rolled Back)",
        error: error.message,
        stack: error.stack,
      })
    );
    res.status(500).json({ message: "Error processing order." });
  }
});

// --- Run Server ---
const run = async () => {
  try {
    await sequelize.authenticate(); // ลองเชื่อมต่อ DB
    console.log(
      JSON.stringify({
        level: "INFO",
        service: SERVICE_NAME,
        message: "PostgreSQL Database connected.",
      })
    );

    app.listen(3000, () => {
      console.log(
        JSON.stringify({
          level: "INFO",
          service: SERVICE_NAME,
          message: "Order API listening on port 3000",
        })
      );
    });
  } catch (e) {
    console.error(
      JSON.stringify({
        level: "FATAL",
        service: SERVICE_NAME,
        message: "Failed to run server or connect to DB",
        error: e.message,
        stack: e.stack,
      })
    );
    process.exit(1); // ออกจากโปรแกรมทันทีถ้าต่อ DB ไม่ได้
  }
};

run();

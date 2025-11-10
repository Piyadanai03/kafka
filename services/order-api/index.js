import express from "express";
import { randomUUID } from "crypto";
import { Order, OutboxEvent } from "./model/models.js";
import { sequelize } from "./db/connectDB.js";
import { SERVICE_NAME } from "./db/connectDB.js";

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

  // ใช้ Database Transaction !!!---
  try {
    // เริ่ม Transaction
    await sequelize.transaction(async (t) => {
      // สร้าง Order (INSERT ลงตาราง 'orders')
      await Order.create(
        {
          id: orderData.orderId,
          sku: orderData.sku,
          quantity: orderData.quantity,
          customer_email: orderData.customerEmail,
        },
        { transaction: t }
      );

      //สร้าง Event (INSERT ลงตาราง 'outbox_events')
      await OutboxEvent.create(
        {
          id: randomUUID(),
          topic: "orders.created", 
          message_key: orderData.orderId, 
          payload: orderData, 
        },
        { transaction: t }
      );

      //ถ้าถึงบรรทัดนี้ได้ -> Sequelize จะ Commit ให้อัตโนมัติ
    });

    //Transaction สำเร็จ! (Commit เรียบร้อย)
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
    // ถ้าเกิด Error -> Sequelize จะ Rollback ให้อัตโนมัติ
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
    await sequelize.authenticate();
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

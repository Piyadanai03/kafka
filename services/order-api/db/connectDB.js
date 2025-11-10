import { Sequelize } from "sequelize";

export const SERVICE_NAME = "order-api";

const sequelize = new Sequelize(
  "order_db", 
  "postgres",
  "postgres", 
  {
    host: "localhost",
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

export { sequelize };
import request from "supertest";
import app from "./server.js";

describe("Sensor APIs", () => {
  it("returns latest data", async () => {
    const res = await request(app).get("/latest");
    expect(res.statusCode).toBe(200);
    expect(res.body).toHaveProperty("time");
  });

  it("returns history", async () => {
    const res = await request(app).get("/history/1");
    expect(Array.isArray(res.body)).toBe(true);
  });

  it("returns analysis", async () => {
    const res = await request(app).get("/analysis/temperature/1");
    expect(res.body).toHaveProperty("mean");
  });
});

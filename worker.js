import WebSocket from "ws";
import fetch from "node-fetch";

const WS_URL = "wss://stream.binance.com:9443/ws/!miniTicker@arr";

const TELEGRAM_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT  = process.env.TELEGRAM_CHAT_ID;

let ws = null;

console.log("PumpTray Worker iniciado");

function sendTelegram(message) {
  if (!TELEGRAM_TOKEN || !TELEGRAM_CHAT) return;

  fetch(`https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      chat_id: TELEGRAM_CHAT,
      text: message
    })
  }).catch(console.error);
}

function connect() {

  if (ws && ws.readyState === 1) return;

  console.log("Conectando WebSocket Binance...");

  ws = new WebSocket(WS_URL);

  ws.on("open", () => {
    console.log("WS conectado");
  });

  ws.on("message", (data) => {

    const tickers = JSON.parse(data);

    for (const t of tickers) {

      const symbol = t.s;
      const price  = parseFloat(t.c);

      if (!symbol || !price) continue;

      if (price > 0) {

        // exemplo simples
        if (symbol === "BTCUSDT") {
          console.log("BTC preço:", price);
        }

      }
    }
  });

  ws.on("close", () => {
    console.log("WS fechado — reconectando...");
    setTimeout(connect, 3000);
  });

  ws.on("error", (err) => {
    console.log("Erro WS:", err.message);
  });
}

connect();

setInterval(() => {
  console.log("Worker ativo", new Date().toISOString());
}, 60000);

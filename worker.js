/**
 * PumpTray Worker — Serviço Persistente
 *
 * Arquitetura:
 * - WebSocket Binance inicia ao carregar o módulo (module-level, persiste no isolate)
 * - alertRules são carregadas do DB no keepalive (a cada 5min) e usadas em tempo real
 * - Alertas Telegram são disparados INSTANTANEAMENTE ao detectar pump
 * - Eventos são enfileirados e salvos no DB no próximo keepalive ping
 */
import WebSocket from "ws";
import fetch from "node-fetch";
import { createClientFromRequest } from 'npm:@base44/sdk@0.8.20';

const WS_URL          = "wss://stream.binance.com:9443/ws/!miniTicker@arr";
const PRICE_WINDOW_MS = 90000;       // 90s de histórico por símbolo
const GLOBAL_COOLDOWN = 60000;       // cooldown detecção global: 1 min
const MAX_BACKOFF     = 30000;

// ── Estado global (persiste no isolate Deno entre requests) ─────────────────
const g = {
  wsConnected:       false,
  pairsMonitored:    0,
  messagesProcessed: 0,
  lastPumpAt:        null,
  startTime:         Date.now(),
  reconnectAttempts: 0,
};

const priceHistory      = new Map(); // symbol -> [{ts, price, q}]
const globalCooldowns   = new Map(); // symbol -> lastAlertTs (ms)
const ruleCooldowns     = new Map(); // ruleId -> lastAlertTs (ms)
const pendingEvents     = [];        // fila de eventos para salvar no DB
let   alertRules        = [];        // carregadas pelo keepalive
let   ws                = null;

// ── Telegram ─────────────────────────────────────────────────────────────────
async function sendTelegram(pump, typeLabel) {
  const token  = Deno.env.get('TELEGRAM_BOT_TOKEN');
  const chatId = Deno.env.get('TELEGRAM_CHAT_ID');
  if (!token || !chatId) return;

  const time  = new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' });
  const fmt   = (v) => v != null ? `${v >= 0 ? '+' : ''}${parseFloat(v).toFixed(3)}%` : 'N/A';
  const label = typeLabel || (pump.status === 'strong_pump' ? 'PUMP FORTE' : 'PUMP');
  const emoji = pump.status === 'strong_pump' ? '🚀🔥' : '🚨';

  const text =
    `${emoji} *PumpTray Alert*\n\n` +
    `Par: \`${pump.symbol}\`\n` +
    `Preço: \`${pump.price}\`\n\n` +
    `5s:  \`${fmt(pump.var5s)}\`\n` +
    `10s: \`${fmt(pump.var10s)}\`\n` +
    `30s: \`${fmt(pump.var30s)}\`\n\n` +
    `Volume Spike: \`${parseFloat(pump.volume_spike || 1).toFixed(2)}x\`\n` +
    `Tipo: \`${label}\`\n\n` +
    `⏰ \`${time}\``;

  try {
    const res  = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ chat_id: chatId, text, parse_mode: 'Markdown' }),
    });
    const json = await res.json();
    if (!json.ok) console.error('[Worker] Telegram erro:', JSON.stringify(json));
    else          console.log(`[Worker] 📱 Telegram: ${pump.symbol} | ${label}`);
  } catch (e) {
    console.error('[Worker] Telegram falhou:', e.message);
  }
}

// ── Verificar AlertRules para um tick ─────────────────────────────────────────
function checkAlertRules(symbol, price, var5s, var10s, var30s, volSpike, score) {
  const now   = Date.now();
  const rules = alertRules.filter(r => r.enabled && r.symbol?.toUpperCase() === symbol);

  const TYPE_LABELS = {
    movement:    'MOVIMENTO',
    pre_pump:    'PRÉ-PUMP',
    pump:        'PUMP',
    strong_pump: 'PUMP FORTE',
    custom:      'PERSONALIZADO',
  };

  for (const rule of rules) {
    const cooldownMs = (rule.cooldown_seconds || 60) * 1000;
    const lastAlert  = ruleCooldowns.get(rule.id) || 0;
    if (now - lastAlert < cooldownMs) continue;

    // Modo personalizado: calcula variação na janela de minutos definida pelo usuário
    if (rule.alert_type === 'custom') {
      const windowMs  = (rule.custom_minutes || 2) * 60 * 1000;
      const targetPct = rule.custom_percent || 0;
      const h         = priceHistory.get(symbol);
      if (!h || h.length < 2) continue;
      const oldest = h.find(v => v.ts >= now - windowMs);
      if (!oldest) continue;
      const varCustom = ((price - oldest.price) / oldest.price) * 100;
      if (varCustom < targetPct) continue;
    } else {
      // Verificar condições preset (apenas as que foram definidas > 0)
      if (rule.min_var5s  > 0 && (var5s  == null || var5s  < rule.min_var5s))  continue;
      if (rule.min_var10s > 0 && (var10s == null || var10s < rule.min_var10s)) continue;
      if (rule.min_var30s > 0 && (var30s == null || var30s < rule.min_var30s)) continue;
    }
    if ((rule.min_volume_spike || 1) > 1 && volSpike < rule.min_volume_spike) continue;

    // ✅ Alerta disparado!
    ruleCooldowns.set(rule.id, now);
    g.lastPumpAt = new Date().toISOString();

    const event = {
      symbol, price,
      var5s:        var5s  ?? null,
      var10s:       var10s ?? null,
      var30s:       var30s ?? null,
      volume_spike: volSpike,
      score,
      status:       rule.alert_type || 'pump',
      timestamp:    new Date().toISOString(),
      alert_rule_id: rule.id,
      trigger_type:  'rule',
    };

    const label = TYPE_LABELS[rule.alert_type] || rule.alert_type;
    console.log(`[Worker] 🔔 Rule: ${rule.alert_type} | ${symbol} | rule=${rule.id}`);

    if (rule.telegram_enabled !== false) {
      sendTelegram(event, label).catch(console.error);
    }
    pendingEvents.push(event);
  }
}

// ── Detecção global (thresholds padrão) ──────────────────────────────────────
function checkGlobalPump(symbol, price, var5s, var10s, var30s, volSpike, score) {
  let status = null;
  if (var30s !== null && var30s >= 3.0)  status = 'strong_pump';
  else if (var10s !== null && var10s >= 1.0) status = 'pump';
  if (!status) return;

  const now       = Date.now();
  const lastAlert = globalCooldowns.get(symbol) || 0;
  if (now - lastAlert < GLOBAL_COOLDOWN) return;
  globalCooldowns.set(symbol, now);

  g.lastPumpAt = new Date().toISOString();
  console.log(`[Worker] 🚀 GLOBAL: ${symbol} | ${status} | 30s: ${(var30s ?? 0).toFixed(2)}%`);

  const event = {
    symbol, price,
    var5s:        var5s  ?? null,
    var10s:       var10s ?? null,
    var30s:       var30s ?? null,
    volume_spike: volSpike,
    score,
    status,
    timestamp:   new Date().toISOString(),
    trigger_type: 'global',
  };

  sendTelegram(event).catch(console.error);
  pendingEvents.push(event);
}

// ── Processar tick ────────────────────────────────────────────────────────────
function processTick(symbol, price, q) {
  const now = Date.now();
  const cut = now - PRICE_WINDOW_MS;

  if (!priceHistory.has(symbol)) priceHistory.set(symbol, []);
  const h = priceHistory.get(symbol);
  h.push({ ts: now, price, q });
  while (h.length && h[0].ts < cut) h.shift();

  g.pairsMonitored = priceHistory.size;
  if (h.length < 5) return;

  const getVar = (sec) => {
    const targetTs = now - sec * 1000;
    for (let i = h.length - 2; i >= 0; i--) {
      if (h[i].ts <= targetTs) return ((price - h[i].price) / h[i].price) * 100;
    }
    return null;
  };

  const var5s  = getVar(5);
  const var10s = getVar(10);
  const var30s = getVar(30);

  // Volume spike
  let volSpike = 1;
  const last   = h[h.length - 1];
  const rBase  = h.find(v => v.ts >= now - 15000);
  const bStart = h.find(v => v.ts >= now - 60000);
  const bEnd   = h.find(v => v.ts >= now - 30000);
  if (rBase && last && bStart && bEnd) {
    const rd = Math.max(0, last.q - rBase.q);
    const bd = Math.max(0, bEnd.q - bStart.q);
    if (bd > 0) volSpike = Math.min(10, parseFloat((rd / bd).toFixed(2)));
  }

  const score =
    (var5s  ?? 0) * 1.0 +
    (var10s ?? 0) * 1.4 +
    (var30s ?? 0) * 1.8 +
    Math.log(volSpike + 1) * 2;

  // 1. Verificar AlertRules personalizadas
  checkAlertRules(symbol, price, var5s, var10s, var30s, volSpike, score);

  // 2. Detecção global padrão
  checkGlobalPump(symbol, price, var5s, var10s, var30s, volSpike, score);
}

// ── WebSocket persistente ────────────────────────────────────────────────────
function connectWs() {
  if (ws && (ws.readyState === 0 || ws.readyState === 1)) return;

  console.log(`[Worker] Conectando WS (tentativa ${g.reconnectAttempts + 1})...`);
  console.log('[Worker] WebSocket URL:', WS_URL);
  ws = new WebSocket(WS_URL);

  ws.onopen = () => {
    g.wsConnected       = true;
    g.reconnectAttempts = 0;
    console.log('[Worker] ✅ WS conectado — monitorando mercado em tempo real');
  };

  ws.onmessage = (e) => {
    try {
      const tickers = JSON.parse(e.data);
      if (!Array.isArray(tickers)) return;
      g.messagesProcessed += tickers.length;
      if (g.messagesProcessed % 500 === 0) {
        console.log(`[Worker] ticks recebidos: ${g.messagesProcessed} acumulados`);
      }
      for (const t of tickers) {
        const price = parseFloat(t.c);
        const q     = parseFloat(t.q || 0);
        if (t.s && price) processTick(t.s, price, q);
      }
    } catch (err) {
      console.error('[Worker] Parse error:', err.message);
    }
  };

  ws.onclose = () => {
    g.wsConnected = false;
    g.reconnectAttempts++;
    const delay = Math.min(1000 * Math.pow(2, g.reconnectAttempts - 1), MAX_BACKOFF);
    console.warn(`[Worker] WS fechado — reconectando em ${delay}ms (tentativa ${g.reconnectAttempts})`);
    setTimeout(connectWs, delay);
  };

  ws.onerror = (err) => {
    console.error('[Worker] WS erro:', err?.message || 'unknown');
  };
}

// ── Inicialização do worker ──────────────────────────────────────────────────
console.log('[Worker] PumpTray Worker iniciado');

connectWs();

// ── HTTP Handler (keepalive + refresh rules + flush DB) ───────────────────────
Deno.serve(async (req) => {
  try {
    const base44 = createClientFromRequest(req);

    // Garantir WS ativo (re-connect se o isolate foi reciclado)
    connectWs();

    // Refresh AlertRules do DB
    try {
      const all  = await base44.asServiceRole.entities.AlertRule.list();
      alertRules = all.filter(r => r.enabled);
      console.log(`[Worker] 📋 AlertRules: ${alertRules.length} regras ativas`);
    } catch (e) {
      console.warn('[Worker] Não foi possível carregar AlertRules:', e.message);
    }

    // Flush de eventos pendentes para o DB
    let flushed = 0;
    if (pendingEvents.length > 0) {
      const toFlush = pendingEvents.splice(0, pendingEvents.length);
      for (const event of toFlush) {
        await base44.asServiceRole.entities.PumpEvent.create(event);
        flushed++;
      }
      if (flushed > 0) console.log(`[Worker] 💾 DB flush: ${flushed} eventos salvos`);
    }

    return Response.json({
      status:              'running',
      websocket_connected: g.wsConnected,
      pairs_monitored:     g.pairsMonitored,
      messages_processed:  g.messagesProcessed,
      uptime_seconds:      Math.round((Date.now() - g.startTime) / 1000),
      last_pump_at:        g.lastPumpAt,
      events_flushed:      flushed,
      alert_rules_loaded:  alertRules.length,
      reconnect_attempts:  g.reconnectAttempts,
    });
  } catch (err) {
    console.error('[Worker] Handler erro:', err.message);
    return Response.json({ error: err.message }, { status: 500 });
  }
});
#!/usr/bin/env node
/* eslint-disable no-console */

const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const { Pool } = require('pg');
const crypto = require('crypto');
const os = require('os');

// ----- WORKER THREAD CODE -----
if (!isMainThread) {
  const { height, config } = workerData;
  
  // Worker-specific functions
  async function fetchBlockResults(height, rpcPrefix) {
    try {
      const res = await fetch(`${rpcPrefix}${height}`, { method: 'GET' });
      if (res.ok) {
        const json = await res.json();
        if (json && (json.result || json.height || json.txs_results)) {
          return wrapResult(json.result ? json.result : json);
        }
      }
    } catch (e) {
      // ignore; fallback to JSON-RPC next
    }

    try {
      const res = await fetch(rpcPrefix, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          jsonrpc: '2.0',
          id: 1,
          method: 'block_results',
          params: { height: height.toString() }
        })
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      if (json.error) throw new Error(json.error.message || 'RPC error');
      return wrapResult(json.result);
    } catch (e) {
      throw new Error(`Failed to fetch block_results for height ${height}: ${e.message}`);
    }
  }

  async function fetchBlockData(height, blockRpcPrefix) {
    const url = `${blockRpcPrefix}${height}`;
    const res = await fetch(url, { method: 'GET' });
    if (!res.ok) throw new Error(`HTTP ${res.status} from ${url}`);
    const json = await res.json();

    const txs = json?.result?.block?.data?.txs;
    const headerTime = json?.result?.block?.header?.time || null;

    let hashes = [];
    if (Array.isArray(txs)) {
      for (const base64Tx of txs) {
        try {
          const buf = Buffer.from(base64Tx, 'base64');
          const h = crypto.createHash('sha256').update(buf).digest('hex');
          hashes.push(h);
        } catch {
          hashes.push(null);
        }
      }
    }

    return { hashes, headerTime };
  }

  function wrapResult(r) {
    return {
      height: asNumber(r.height),
      txs_results: Array.isArray(r.txs_results) ? r.txs_results : [],
      finalize_block_events: Array.isArray(r.finalize_block_events) ? r.finalize_block_events : [],
      app_hash: r.app_hash || null
    };
  }

  function asNumber(x) {
    if (x == null) return null;
    if (typeof x === 'number') return x;
    const n = Number(String(x).replace(/"/g, ''));
    return Number.isFinite(n) ? n : null;
  }

  function maybeB64ToUtf8(s) {
    if (s == null) return null;
    try {
      if (!/^[A-Za-z0-9+/=]+$/.test(s)) return s;
      const buf = Buffer.from(s, 'base64');
      const re = buf.toString('base64').replace(/=+$/, '');
      const inNorm = s.replace(/=+$/, '');
      if (re !== inNorm) return s;
      const decoded = buf.toString('utf8');
      const nonPrintableRatio = decoded.replace(/[\x20-\x7E\t\r\n]/g, '').length / Math.max(decoded.length, 1);
      if (decoded.length > 0 && nonPrintableRatio > 0.2) return s;
      return decoded;
    } catch {
      return s;
    }
  }

  function buildEventJson(attrsArray) {
    const attrs_kv = [];
    const map = Object.create(null);

    const attrs = Array.isArray(attrsArray) ? attrsArray : [];
    for (let idx = 0; idx < attrs.length; idx++) {
      const a = attrs[idx] || {};
      const key = maybeB64ToUtf8(a.key);
      const value = maybeB64ToUtf8(a.value);
      const indexed = a.index === true;

      attrs_kv.push({
        key,
        value,
        indexed,
        attr_index: idx
      });

      if (!map[key]) map[key] = [];
      map[key].push(value);
    }

    return {
      attrs_kv,
      attrs_map: map,
      attr_count: attrs_kv.length
    };
  }

  // Process the assigned height
  (async () => {
    try {
      const pool = new Pool({ connectionString: config.pg });
      const client = await pool.connect();
      
      try {
        const [r, blockData] = await Promise.all([
          fetchBlockResults(height, config.rpc),
          fetchBlockData(height, config.blockRpc)
        ]);

        const txHashes = blockData.hashes;
        const blockTime = blockData.headerTime ? new Date(blockData.headerTime) : null;

        if (txHashes.length && txHashes.length !== r.txs_results.length) {
          console.warn(`⚠️ Height ${height}: tx count mismatch. block.txs=${txHashes.length} vs block_results.txs_results=${r.txs_results.length}`);
        }

        await client.query('BEGIN');

        // SQL queries
        const SQL = {
          upsertBlock: `
            INSERT INTO blocks (height, app_hash, txs_results_count, finalize_events_count, created_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (height) DO UPDATE
              SET app_hash = EXCLUDED.app_hash,
                  txs_results_count = EXCLUDED.txs_results_count,
                  finalize_events_count = EXCLUDED.finalize_events_count,
                  created_at = EXCLUDED.created_at;
          `,
          upsertTx: `
            INSERT INTO txs (height, tx_index, tx_hash, code, gas_wanted, gas_used, data, log)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (height, tx_index) DO UPDATE
              SET tx_hash = EXCLUDED.tx_hash,
                  code = EXCLUDED.code,
                  gas_wanted = EXCLUDED.gas_wanted,
                  gas_used = EXCLUDED.gas_used,
                  data = EXCLUDED.data,
                  log = EXCLUDED.log;
          `,
          upsertTxEvent: `
            INSERT INTO tx_events (height, tx_index, event_index, type)
            VALUES ($1,$2,$3,$4)
            ON CONFLICT (height, tx_index, event_index) DO UPDATE
              SET type = EXCLUDED.type;
          `,
          upsertTxEventJson: `
            INSERT INTO tx_event_attrs_json (height, tx_index, event_index, attrs_kv, attrs_map, attr_count)
            VALUES ($1,$2,$3,$4::jsonb,$5::jsonb,$6)
            ON CONFLICT (height, tx_index, event_index) DO UPDATE
              SET attrs_kv = EXCLUDED.attrs_kv,
                  attrs_map = EXCLUDED.attrs_map,
                  attr_count = EXCLUDED.attr_count;
          `,
          upsertBlockEvent: `
            INSERT INTO block_events (height, event_index, type)
            VALUES ($1,$2,$3)
            ON CONFLICT (height, event_index) DO UPDATE
              SET type = EXCLUDED.type;
          `,
          upsertBlockEventAttr: `
            INSERT INTO block_event_attrs (height, event_index, attr_index, key, value, "indexed")
            VALUES ($1,$2,$3,$4,$5,$6)
            ON CONFLICT (height, event_index, attr_index) DO UPDATE
              SET key = EXCLUDED.key, value = EXCLUDED.value, "indexed" = EXCLUDED."indexed";
          `
        };

        // 1) block row
        const appHashBuf = r.app_hash ? Buffer.from(r.app_hash.replace(/^0x/i, ''), 'hex') : null;
        await client.query(SQL.upsertBlock, [
          r.height,
          appHashBuf,
          r.txs_results.length,
          r.finalize_block_events.length,
          blockTime
        ]);

        // 2) txs + events (JSONB)
        for (let i = 0; i < r.txs_results.length; i++) {
          const tx = r.txs_results[i] || {};
          const dataBuf = tx.data ? Buffer.from(tx.data, 'base64') : null;
          const tx_hash = txHashes[i] || null;

          await client.query(SQL.upsertTx, [
            r.height,
            i,
            tx_hash,
            tx.code ?? null,
            asNumber(tx.gas_wanted),
            asNumber(tx.gas_used),
            dataBuf,
            typeof tx.log === 'string' ? tx.log : (tx.log ? JSON.stringify(tx.log) : null)
          ]);

          const events = Array.isArray(tx.events) ? tx.events : [];
          for (let eIdx = 0; eIdx < events.length; eIdx++) {
            const ev = events[eIdx] || {};
            const type = ev.type || 'unknown';
            await client.query(SQL.upsertTxEvent, [r.height, i, eIdx, type]);

            const { attrs_kv, attrs_map, attr_count } = buildEventJson(ev.attributes);
            await client.query(SQL.upsertTxEventJson, [
              r.height,
              i,
              eIdx,
              JSON.stringify(attrs_kv),
              JSON.stringify(attrs_map),
              attr_count
            ]);
          }
        }

        // 3) finalize_block_events
        const bevents = Array.isArray(r.finalize_block_events) ? r.finalize_block_events : [];
        for (let eIdx = 0; eIdx < bevents.length; eIdx++) {
          const ev = bevents[eIdx] || {};
          const type = ev.type || 'unknown';
          await client.query(SQL.upsertBlockEvent, [r.height, eIdx, type]);

          const attrs = Array.isArray(ev.attributes) ? ev.attributes : [];
          for (let aIdx = 0; aIdx < attrs.length; aIdx++) {
            const { key, value, index } = attrs[aIdx] || {};
            await client.query(SQL.upsertBlockEventAttr, [
              r.height,
              eIdx,
              aIdx,
              maybeB64ToUtf8(key),
              maybeB64ToUtf8(value),
              index === true
            ]);
          }
        }

        await client.query('COMMIT');
        parentPort.postMessage({ success: true, height });
        
      } catch (error) {
        await client.query('ROLLBACK').catch(() => {});
        parentPort.postMessage({ success: false, height, error: error.message });
      } finally {
        client.release();
        await pool.end();
      }
    } catch (error) {
      parentPort.postMessage({ success: false, height, error: error.message });
    }
  })();

  return;
}

// ----- MAIN THREAD CODE -----

const args = require('minimist')(process.argv.slice(2), {
  string: ['rpc', 'blockRpc', 'from', 'to', 'pg'],
  number: ['concurrency', 'workers'],
  default: {
    rpc: 'https://public-zigchain-testnet-rpc.numia.xyz/block_results?height=',
    blockRpc: 'https://public-zigchain-testnet-rpc.numia.xyz/block?height=',
    pg: process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/chain',
    concurrency: os.cpus().length,
    workers: os.cpus().length
  }
});

const FROM = parseInt(args.from || '2086230', 10);
const TO = parseInt(args.to || '2086240', 10);
const WORKERS = Math.min(args.workers, os.cpus().length * 2); // Cap at 2x CPU cores
const CONCURRENCY = args.concurrency;

console.log(`🚀 Starting optimized block ingestion:`);
console.log(`   Heights: ${FROM} to ${TO} (${TO - FROM + 1} blocks)`);
console.log(`   Workers: ${WORKERS}`);
console.log(`   Concurrency per batch: ${CONCURRENCY}`);
console.log(`   Total CPU cores: ${os.cpus().length}`);
console.log(`   RPC: ${args.rpc}`);
console.log(`   Block RPC: ${args.blockRpc}`);
console.log(`   Database: ${args.pg}\n`);

class WorkerPool {
  constructor(workerCount, config) {
    this.workers = [];
    this.queue = [];
    this.activeJobs = 0;
    this.config = config;
    this.results = {
      success: 0,
      failed: 0,
      total: 0
    };
    
    // Create workers
    for (let i = 0; i < workerCount; i++) {
      this.createWorker();
    }
  }

  createWorker() {
    const worker = {
      thread: null,
      busy: false,
      id: this.workers.length
    };
    this.workers.push(worker);
  }

  async processHeight(height) {
    return new Promise((resolve) => {
      this.queue.push({ height, resolve });
      this.processQueue();
    });
  }

  processQueue() {
    if (this.queue.length === 0) return;
    
    const availableWorker = this.workers.find(w => !w.busy);
    if (!availableWorker) return;

    const job = this.queue.shift();
    availableWorker.busy = true;
    this.activeJobs++;

    // Create new worker thread for this job
    availableWorker.thread = new Worker(__filename, {
      workerData: {
        height: job.height,
        config: this.config
      }
    });

    availableWorker.thread.on('message', (result) => {
      this.results.total++;
      if (result.success) {
        this.results.success++;
        console.log(`✔️  [Worker ${availableWorker.id}] Height ${result.height} completed (${this.results.success}/${this.results.total})`);
      } else {
        this.results.failed++;
        console.error(`✖️  [Worker ${availableWorker.id}] Height ${result.height} failed: ${result.error}`);
      }
      
      // Clean up worker
      availableWorker.thread.terminate();
      availableWorker.thread = null;
      availableWorker.busy = false;
      this.activeJobs--;
      
      job.resolve(result);
      
      // Process next item in queue
      process.nextTick(() => this.processQueue());
    });

    availableWorker.thread.on('error', (error) => {
      this.results.total++;
      this.results.failed++;
      console.error(`✖️  [Worker ${availableWorker.id}] Height ${job.height} worker error: ${error.message}`);
      
      availableWorker.thread = null;
      availableWorker.busy = false;
      this.activeJobs--;
      
      job.resolve({ success: false, height: job.height, error: error.message });
      
      process.nextTick(() => this.processQueue());
    });
  }

  async waitForCompletion() {
    while (this.activeJobs > 0 || this.queue.length > 0) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  getStats() {
    return this.results;
  }
}

async function main() {
  const startTime = Date.now();
  
  const workerPool = new WorkerPool(WORKERS, {
    rpc: args.rpc,
    blockRpc: args.blockRpc,
    pg: args.pg
  });

  // Create array of all heights to process
  const heights = [];
  for (let h = FROM; h <= TO; h++) {
    heights.push(h);
  }

  // Process heights in batches with concurrency control
  const batchSize = CONCURRENCY;
  console.log(`📦 Processing ${heights.length} heights in batches of ${batchSize}\n`);

  for (let i = 0; i < heights.length; i += batchSize) {
    const batch = heights.slice(i, i + batchSize);
    const batchPromises = batch.map(height => workerPool.processHeight(height));
    
    // Wait for current batch to complete before starting next batch
    await Promise.all(batchPromises);
    
    const stats = workerPool.getStats();
    const progress = ((i + batch.length) / heights.length * 100).toFixed(1);
    console.log(`📊 Batch completed. Progress: ${progress}% (${stats.success} success, ${stats.failed} failed)\n`);
  }

  // Wait for any remaining work to complete
  await workerPool.waitForCompletion();

  const endTime = Date.now();
  const duration = (endTime - startTime) / 1000;
  const stats = workerPool.getStats();
  
  console.log(`\n🎉 Ingestion completed!`);
  console.log(`   Total time: ${duration.toFixed(2)}s`);
  console.log(`   Blocks processed: ${stats.total}`);
  console.log(`   Successful: ${stats.success}`);
  console.log(`   Failed: ${stats.failed}`);
  console.log(`   Rate: ${(stats.total / duration).toFixed(2)} blocks/second`);
  console.log(`   Average time per block: ${(duration / stats.total * 1000).toFixed(0)}ms`);
}

if (isMainThread) {
  main().catch((error) => {
    console.error('Fatal error:', error);
    process.exit(1);
  });
}

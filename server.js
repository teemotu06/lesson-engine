require('dotenv').config();

const express = require('express');
const { Pool } = require('pg');
const path = require('path');

const app  = express();
const PORT = process.env.PORT || 3000;

// ─── Database ──────────────────────────────────────────────────────────────
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL && !process.env.DATABASE_URL.includes('localhost')
    ? { rejectUnauthorized: false }
    : false
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS classes  (id TEXT PRIMARY KEY, data JSONB NOT NULL);
    CREATE TABLE IF NOT EXISTS units    (id TEXT PRIMARY KEY, data JSONB NOT NULL);
    CREATE TABLE IF NOT EXISTS lessons  (id TEXT PRIMARY KEY, data JSONB NOT NULL);
    CREATE TABLE IF NOT EXISTS logs     (id TEXT PRIMARY KEY, data JSONB NOT NULL);
  `);
}

// ─── Middleware ────────────────────────────────────────────────────────────
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── Classes ───────────────────────────────────────────────────────────────
app.get('/api/classes', async (req, res) => {
  const { rows } = await pool.query('SELECT data FROM classes');
  res.json(rows.map(r => r.data));
});

app.post('/api/classes', async (req, res) => {
  await pool.query(
    'INSERT INTO classes (id, data) VALUES ($1, $2)',
    [req.body.id, req.body]
  );
  res.json({ ok: true });
});

app.delete('/api/classes/:id', async (req, res) => {
  const { id } = req.params;
  const { rows } = await pool.query(
    "SELECT id FROM lessons WHERE data->>'classId' = $1", [id]
  );
  for (const r of rows) {
    await pool.query("DELETE FROM logs WHERE data->>'lessonId' = $1", [r.id]);
  }
  await pool.query("DELETE FROM lessons WHERE data->>'classId' = $1", [id]);
  await pool.query("DELETE FROM units    WHERE data->>'classId' = $1", [id]);
  await pool.query('DELETE FROM classes WHERE id = $1', [id]);
  res.json({ ok: true });
});

// ─── Units ─────────────────────────────────────────────────────────────────
app.get('/api/units', async (req, res) => {
  const { rows } = await pool.query('SELECT data FROM units');
  res.json(rows.map(r => r.data));
});

app.post('/api/units', async (req, res) => {
  await pool.query(
    'INSERT INTO units (id, data) VALUES ($1, $2)',
    [req.body.id, req.body]
  );
  res.json({ ok: true });
});

app.delete('/api/units/:id', async (req, res) => {
  const { id } = req.params;
  const { rows } = await pool.query(
    "SELECT id FROM lessons WHERE data->>'unitId' = $1", [id]
  );
  for (const r of rows) {
    await pool.query("DELETE FROM logs WHERE data->>'lessonId' = $1", [r.id]);
  }
  await pool.query("DELETE FROM lessons WHERE data->>'unitId' = $1", [id]);
  await pool.query('DELETE FROM units WHERE id = $1', [id]);
  res.json({ ok: true });
});

// ─── Lessons ───────────────────────────────────────────────────────────────
app.get('/api/lessons', async (req, res) => {
  const { rows } = await pool.query('SELECT data FROM lessons');
  res.json(rows.map(r => r.data));
});

app.post('/api/lessons', async (req, res) => {
  await pool.query(
    'INSERT INTO lessons (id, data) VALUES ($1, $2)',
    [req.body.id, req.body]
  );
  res.json({ ok: true });
});

app.delete('/api/lessons/:id', async (req, res) => {
  const { id } = req.params;
  await pool.query("DELETE FROM logs    WHERE data->>'lessonId' = $1", [id]);
  await pool.query('DELETE FROM lessons WHERE id = $1', [id]);
  res.json({ ok: true });
});

// ─── Logs ──────────────────────────────────────────────────────────────────
app.get('/api/logs', async (req, res) => {
  const { rows } = await pool.query('SELECT data FROM logs');
  res.json(rows.map(r => r.data));
});

app.post('/api/logs', async (req, res) => {
  await pool.query(
    'INSERT INTO logs (id, data) VALUES ($1, $2)',
    [req.body.id, req.body]
  );
  res.json({ ok: true });
});

// ─── Export ────────────────────────────────────────────────────────────────
app.get('/api/export', async (req, res) => {
  const [c, u, l, g] = await Promise.all([
    pool.query('SELECT data FROM classes'),
    pool.query('SELECT data FROM units'),
    pool.query('SELECT data FROM lessons'),
    pool.query('SELECT data FROM logs'),
  ]);
  res.json({
    classes:  c.rows.map(r => r.data),
    units:    u.rows.map(r => r.data),
    lessons:  l.rows.map(r => r.data),
    logs:     g.rows.map(r => r.data),
  });
});

// ─── Import ────────────────────────────────────────────────────────────────
app.post('/api/import', async (req, res) => {
  const { classes = [], units = [], lessons = [], logs = [] } = req.body;
  await pool.query('DELETE FROM logs');
  await pool.query('DELETE FROM lessons');
  await pool.query('DELETE FROM units');
  await pool.query('DELETE FROM classes');
  for (const c of classes) await pool.query('INSERT INTO classes (id, data) VALUES ($1, $2)', [c.id, c]);
  for (const u of units)   await pool.query('INSERT INTO units   (id, data) VALUES ($1, $2)', [u.id, u]);
  for (const l of lessons) await pool.query('INSERT INTO lessons (id, data) VALUES ($1, $2)', [l.id, l]);
  for (const g of logs)    await pool.query('INSERT INTO logs    (id, data) VALUES ($1, $2)', [g.id, g]);
  res.json({ ok: true });
});

// ─── Clear ─────────────────────────────────────────────────────────────────
app.delete('/api/clear', async (req, res) => {
  await pool.query('DELETE FROM logs');
  await pool.query('DELETE FROM lessons');
  await pool.query('DELETE FROM units');
  await pool.query('DELETE FROM classes');
  res.json({ ok: true });
});

// ─── Start ─────────────────────────────────────────────────────────────────
initDB()
  .then(() => app.listen(PORT, () => console.log(`Lesson Engine running on port ${PORT}`)))
  .catch(err => { console.error('DB init failed:', err); process.exit(1); });

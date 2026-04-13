require('dotenv').config();

const express = require('express');
const { Pool } = require('pg');
const path = require('path');
const fs = require('fs/promises');

const app = express();
const PORT = process.env.PORT || 3000;
const DATA_FILE = path.join(__dirname, 'data.local.json');

function normalizeDataShape(data) {
  const classes = Array.isArray(data.classes) ? data.classes : [];
  const units = Array.isArray(data.units) ? data.units : [];
  const subunits = Array.isArray(data.subunits) ? data.subunits : [];
  const lessons = Array.isArray(data.lessons) ? data.lessons : [];
  const logs = Array.isArray(data.logs) ? data.logs : [];
  const studentnotes = Array.isArray(data.studentnotes) ? data.studentnotes : [];
  const attendances = Array.isArray(data.attendances) ? data.attendances : [];
  const schoolsettings = Array.isArray(data.schoolsettings)
    ? data.schoolsettings
    : (data.schoolSettings ? [data.schoolSettings] : [{ id: 'school', schoolName: '', holidays: [] }]);
  const rawStudents = Array.isArray(data.students) ? data.students : [];
  const explicitEnrollments = Array.isArray(data.enrollments) ? data.enrollments : [];

  const students = rawStudents.map(student => {
    const { classId, ...rest } = student || {};
    return rest;
  }).filter(student => student && student.id);

  const seenEnrollmentIds = new Set();
  const enrollments = [];

  for (const enrollment of explicitEnrollments) {
    if (!enrollment || !enrollment.id || !enrollment.studentId || !enrollment.classId || seenEnrollmentIds.has(enrollment.id)) continue;
    seenEnrollmentIds.add(enrollment.id);
    enrollments.push(enrollment);
  }

  for (const student of rawStudents) {
    if (!student || !student.id || !student.classId) continue;
    const enrollmentId = `en_${student.id}_${student.classId}`;
    if (seenEnrollmentIds.has(enrollmentId)) continue;
    seenEnrollmentIds.add(enrollmentId);
    enrollments.push({ id: enrollmentId, studentId: student.id, classId: student.classId });
  }

  return { classes, units, subunits, lessons, logs, students, enrollments, studentnotes, attendances, schoolsettings };
}

function createPostgresStore(pool) {
  return {
    async init() {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS classes  (id TEXT PRIMARY KEY, data JSONB NOT NULL);
        CREATE TABLE IF NOT EXISTS units    (id TEXT PRIMARY KEY, data JSONB NOT NULL);
        CREATE TABLE IF NOT EXISTS subunits (id TEXT PRIMARY KEY, data JSONB NOT NULL);
        CREATE TABLE IF NOT EXISTS lessons  (id TEXT PRIMARY KEY, data JSONB NOT NULL);
        CREATE TABLE IF NOT EXISTS logs         (id TEXT PRIMARY KEY, data JSONB NOT NULL);
        CREATE TABLE IF NOT EXISTS students     (id TEXT PRIMARY KEY, data JSONB NOT NULL);
        CREATE TABLE IF NOT EXISTS enrollments  (id TEXT PRIMARY KEY, data JSONB NOT NULL);
        CREATE TABLE IF NOT EXISTS studentnotes (id TEXT PRIMARY KEY, data JSONB NOT NULL);
        CREATE TABLE IF NOT EXISTS attendances  (id TEXT PRIMARY KEY, data JSONB NOT NULL);
        CREATE TABLE IF NOT EXISTS schoolsettings (id TEXT PRIMARY KEY, data JSONB NOT NULL);
      `);
    },
    async list(table) {
      const { rows } = await pool.query(`SELECT data FROM ${table}`);
      return rows.map(row => row.data);
    },
    async upsert(table, entry) {
      await pool.query(
        `INSERT INTO ${table} (id, data) VALUES ($1, $2)
         ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data`,
        [entry.id, entry]
      );
    },
    async remove(table, id) {
      await pool.query(`DELETE FROM ${table} WHERE id = $1`, [id]);
    },
    async removeByField(table, field, value) {
      await pool.query(`DELETE FROM ${table} WHERE data->>$1 = $2`, [field, value]);
    },
    async findIdsByField(table, field, value) {
      const { rows } = await pool.query(
        `SELECT id FROM ${table} WHERE data->>$1 = $2`,
        [field, value]
      );
      return rows.map(row => row.id);
    },
    async replaceAll(data) {
      await pool.query('BEGIN');
      try {
        await pool.query('DELETE FROM studentnotes');
        await pool.query('DELETE FROM attendances');
        await pool.query('DELETE FROM enrollments');
        await pool.query('DELETE FROM schoolsettings');
        await pool.query('DELETE FROM students');
        await pool.query('DELETE FROM logs');
        await pool.query('DELETE FROM lessons');
        await pool.query('DELETE FROM subunits');
        await pool.query('DELETE FROM units');
        await pool.query('DELETE FROM classes');

        for (const entry of data.classes)      await this.upsert('classes',      entry);
        for (const entry of data.units)        await this.upsert('units',        entry);
        for (const entry of data.subunits)     await this.upsert('subunits',     entry);
        for (const entry of data.lessons)      await this.upsert('lessons',      entry);
        for (const entry of data.logs)         await this.upsert('logs',         entry);
        for (const entry of (data.students     || [])) await this.upsert('students',     entry);
        for (const entry of (data.enrollments  || [])) await this.upsert('enrollments',  entry);
        for (const entry of (data.studentnotes || [])) await this.upsert('studentnotes', entry);
        for (const entry of (data.attendances  || [])) await this.upsert('attendances',  entry);
        for (const entry of (data.schoolsettings || [])) await this.upsert('schoolsettings', entry);

        await pool.query('COMMIT');
      } catch (error) {
        await pool.query('ROLLBACK');
        throw error;
      }
    },
    async clearAll() {
      await pool.query('DELETE FROM studentnotes');
      await pool.query('DELETE FROM attendances');
      await pool.query('DELETE FROM enrollments');
      await pool.query('DELETE FROM schoolsettings');
      await pool.query('DELETE FROM students');
      await pool.query('DELETE FROM logs');
      await pool.query('DELETE FROM lessons');
      await pool.query('DELETE FROM subunits');
      await pool.query('DELETE FROM units');
      await pool.query('DELETE FROM classes');
    }
  };
}

function createFileStore(filename) {
  let state = { classes: [], units: [], subunits: [], lessons: [], logs: [], students: [], enrollments: [], studentnotes: [], attendances: [], schoolsettings: [{ id: 'school', schoolName: '', holidays: [] }] };
  let writeQueue = Promise.resolve();

  async function ensureFile() {
    try {
      const raw = await fs.readFile(filename, 'utf8');
      state = normalizeDataShape(JSON.parse(raw));
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
      await fs.writeFile(filename, JSON.stringify(state, null, 2));
    }
  }

  function queueWrite() {
    writeQueue = writeQueue.then(() =>
      fs.writeFile(filename, JSON.stringify(state, null, 2))
    );
    return writeQueue;
  }

  return {
    async init() { await ensureFile(); },
    async list(table) { return state[table].map(entry => ({ ...entry })); },
    async upsert(table, entry) {
      const items = state[table];
      const index = items.findIndex(item => item.id === entry.id);
      if (index >= 0) items[index] = entry;
      else items.push(entry);
      await queueWrite();
    },
    async remove(table, id) {
      state[table] = state[table].filter(entry => entry.id !== id);
      await queueWrite();
    },
    async removeByField(table, field, value) {
      state[table] = state[table].filter(entry => entry[field] !== value);
      await queueWrite();
    },
    async findIdsByField(table, field, value) {
      return state[table].filter(entry => entry[field] === value).map(entry => entry.id);
    },
    async replaceAll(data) {
      state = normalizeDataShape(data);
      await queueWrite();
    },
    async clearAll() {
      state = { classes: [], units: [], subunits: [], lessons: [], logs: [], students: [], enrollments: [], studentnotes: [], attendances: [], schoolsettings: [{ id: 'school', schoolName: '', holidays: [] }] };
      await queueWrite();
    }
  };
}

async function createStore() {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    const fileStore = createFileStore(DATA_FILE);
    await fileStore.init();
    console.log(`Lesson Engine using local file storage at ${DATA_FILE}`);
    return fileStore;
  }

  const pool = new Pool({
    connectionString: databaseUrl,
    ssl: databaseUrl.includes('localhost') ? false : { rejectUnauthorized: false }
  });

  const postgresStore = createPostgresStore(pool);
  try {
    await postgresStore.init();
    console.log('Lesson Engine connected to PostgreSQL');
    return postgresStore;
  } catch (error) {
    console.warn(`PostgreSQL unavailable (${error.code || error.message}). Falling back to local file storage.`);
    await pool.end().catch(() => {});
    const fileStore = createFileStore(DATA_FILE);
    await fileStore.init();
    console.log(`Lesson Engine using local file storage at ${DATA_FILE}`);
    return fileStore;
  }
}

async function migrateLegacyStudentAssignments(activeStore) {
  const [students, enrollments] = await Promise.all([
    activeStore.list('students'),
    activeStore.list('enrollments').catch(() => [])
  ]);

  if (enrollments.length > 0) return;

  const legacyStudents = students.filter(student => student && student.classId);
  if (!legacyStudents.length) return;

  for (const student of legacyStudents) {
    const { classId, ...cleanStudent } = student;
    await activeStore.upsert('students', cleanStudent);
    await activeStore.upsert('enrollments', {
      id: `en_${student.id}_${classId}`,
      studentId: student.id,
      classId
    });
  }
}

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

let store;

// ─── Classes ───────────────────────────────────────────────────────────────
app.get('/api/classes', async (req, res, next) => {
  try { res.json(await store.list('classes')); } catch (e) { next(e); }
});

app.post('/api/classes', async (req, res, next) => {
  try { await store.upsert('classes', req.body); res.json({ ok: true }); } catch (e) { next(e); }
});

app.delete('/api/classes/:id', async (req, res, next) => {
  try {
    const { id } = req.params;
    // Cascade: logs → lessons → subunits → units (books) → class
    const lessonIds = await store.findIdsByField('lessons', 'classId', id);
    for (const lessonId of lessonIds) {
      await store.removeByField('logs', 'lessonId', lessonId);
      await store.removeByField('studentnotes', 'lessonId', lessonId);
      await store.removeByField('attendances', 'lessonId', lessonId);
    }
    await store.removeByField('lessons', 'classId', id);
    await store.removeByField('subunits', 'classId', id);
    await store.removeByField('units', 'classId', id);
    await store.removeByField('enrollments', 'classId', id);
    await store.remove('classes', id);
    res.json({ ok: true });
  } catch (e) { next(e); }
});

// ─── Units (Books) ─────────────────────────────────────────────────────────
app.get('/api/units', async (req, res, next) => {
  try { res.json(await store.list('units')); } catch (e) { next(e); }
});

app.post('/api/units', async (req, res, next) => {
  try { await store.upsert('units', req.body); res.json({ ok: true }); } catch (e) { next(e); }
});

app.delete('/api/units/:id', async (req, res, next) => {
  try {
    const { id } = req.params;
    // Cascade: logs → lessons (by bookId) → subunits → book
    const lessonIds = await store.findIdsByField('lessons', 'bookId', id);
    for (const lessonId of lessonIds) {
      await store.removeByField('logs', 'lessonId', lessonId);
    }
    await store.removeByField('lessons', 'bookId', id);
    await store.removeByField('subunits', 'bookId', id);
    await store.remove('units', id);
    res.json({ ok: true });
  } catch (e) { next(e); }
});

// ─── Subunits (Units within Books) ─────────────────────────────────────────
app.get('/api/subunits', async (req, res, next) => {
  try { res.json(await store.list('subunits')); } catch (e) { next(e); }
});

app.post('/api/subunits', async (req, res, next) => {
  try { await store.upsert('subunits', req.body); res.json({ ok: true }); } catch (e) { next(e); }
});

app.delete('/api/subunits/:id', async (req, res, next) => {
  try {
    const { id } = req.params;
    // Cascade: logs → lessons → subunit
    const lessonIds = await store.findIdsByField('lessons', 'subunitId', id);
    for (const lessonId of lessonIds) {
      await store.removeByField('logs', 'lessonId', lessonId);
    }
    await store.removeByField('lessons', 'subunitId', id);
    await store.remove('subunits', id);
    res.json({ ok: true });
  } catch (e) { next(e); }
});

// ─── Lessons ───────────────────────────────────────────────────────────────
app.get('/api/lessons', async (req, res, next) => {
  try { res.json(await store.list('lessons')); } catch (e) { next(e); }
});

app.post('/api/lessons', async (req, res, next) => {
  try { await store.upsert('lessons', req.body); res.json({ ok: true }); } catch (e) { next(e); }
});

app.delete('/api/lessons/:id', async (req, res, next) => {
  try {
    const { id } = req.params;
    await store.removeByField('logs', 'lessonId', id);
    await store.removeByField('studentnotes', 'lessonId', id);
    await store.removeByField('attendances', 'lessonId', id);
    await store.remove('lessons', id);
    res.json({ ok: true });
  } catch (e) { next(e); }
});

// ─── Logs ──────────────────────────────────────────────────────────────────
app.get('/api/logs', async (req, res, next) => {
  try { res.json(await store.list('logs')); } catch (e) { next(e); }
});

app.post('/api/logs', async (req, res, next) => {
  try { await store.upsert('logs', req.body); res.json({ ok: true }); } catch (e) { next(e); }
});

// ─── Students ──────────────────────────────────────────────────────────────
app.get('/api/students', async (req, res, next) => {
  try { res.json(await store.list('students')); } catch (e) { next(e); }
});

app.post('/api/students', async (req, res, next) => {
  try { await store.upsert('students', req.body); res.json({ ok: true }); } catch (e) { next(e); }
});

app.delete('/api/students/:id', async (req, res, next) => {
  try {
    const { id } = req.params;
    await store.removeByField('studentnotes', 'studentId', id);
    await store.removeByField('attendances', 'studentId', id);
    await store.removeByField('enrollments', 'studentId', id);
    await store.remove('students', id);
    res.json({ ok: true });
  } catch (e) { next(e); }
});

// ─── Enrollments ───────────────────────────────────────────────────────────
app.get('/api/enrollments', async (req, res, next) => {
  try { res.json(await store.list('enrollments')); } catch (e) { next(e); }
});

app.post('/api/enrollments', async (req, res, next) => {
  try { await store.upsert('enrollments', req.body); res.json({ ok: true }); } catch (e) { next(e); }
});

app.delete('/api/enrollments/:id', async (req, res, next) => {
  try { await store.remove('enrollments', req.params.id); res.json({ ok: true }); } catch (e) { next(e); }
});

// ─── Student Notes ─────────────────────────────────────────────────────────
app.get('/api/studentnotes', async (req, res, next) => {
  try { res.json(await store.list('studentnotes')); } catch (e) { next(e); }
});

app.post('/api/studentnotes', async (req, res, next) => {
  try { await store.upsert('studentnotes', req.body); res.json({ ok: true }); } catch (e) { next(e); }
});

app.delete('/api/studentnotes/:id', async (req, res, next) => {
  try { await store.remove('studentnotes', req.params.id); res.json({ ok: true }); } catch (e) { next(e); }
});

// ─── Attendance ────────────────────────────────────────────────────────────
app.get('/api/attendances', async (req, res, next) => {
  try { res.json(await store.list('attendances')); } catch (e) { next(e); }
});

app.post('/api/attendances', async (req, res, next) => {
  try { await store.upsert('attendances', req.body); res.json({ ok: true }); } catch (e) { next(e); }
});

app.delete('/api/attendances/:id', async (req, res, next) => {
  try { await store.remove('attendances', req.params.id); res.json({ ok: true }); } catch (e) { next(e); }
});

// ─── School Settings ───────────────────────────────────────────────────────
app.get('/api/school-settings', async (req, res, next) => {
  try {
    const [settings] = await store.list('schoolsettings');
    res.json(settings || { id: 'school', schoolName: '', holidays: [] });
  } catch (e) { next(e); }
});

app.post('/api/school-settings', async (req, res, next) => {
  try {
    await store.upsert('schoolsettings', { id: 'school', ...req.body });
    res.json({ ok: true });
  } catch (e) { next(e); }
});

// ─── Export ────────────────────────────────────────────────────────────────
app.get('/api/export', async (req, res, next) => {
  try {
    const [classes, units, subunits, lessons, logs, students, enrollments, studentnotes, attendances, schoolsettings] = await Promise.all([
      store.list('classes'),
      store.list('units'),
      store.list('subunits'),
      store.list('lessons'),
      store.list('logs'),
      store.list('students'),
      store.list('enrollments'),
      store.list('studentnotes'),
      store.list('attendances'),
      store.list('schoolsettings')
    ]);
    res.json({ classes, units, subunits, lessons, logs, students, enrollments, studentnotes, attendances, schoolSettings: schoolsettings[0] || { id: 'school', schoolName: '', holidays: [] } });
  } catch (e) { next(e); }
});

// ─── Import ────────────────────────────────────────────────────────────────
app.post('/api/import', async (req, res, next) => {
  try {
    const { classes = [], units = [], subunits = [], lessons = [], logs = [], students = [], enrollments = [], studentnotes = [], attendances = [], schoolSettings = null, schoolsettings = [] } = req.body;
    await store.replaceAll({ classes, units, subunits, lessons, logs, students, enrollments, studentnotes, attendances, schoolsettings: schoolsettings.length ? schoolsettings : (schoolSettings ? [schoolSettings] : []) });
    res.json({ ok: true });
  } catch (e) { next(e); }
});

// ─── Clear ─────────────────────────────────────────────────────────────────
app.delete('/api/clear', async (req, res, next) => {
  try { await store.clearAll(); res.json({ ok: true }); } catch (e) { next(e); }
});

app.use((error, req, res, next) => {
  console.error(error);
  res.status(500).json({ error: 'Internal server error' });
});

createStore()
  .then(activeStore => {
    store = activeStore;
    return migrateLegacyStudentAssignments(store).then(() => store);
  })
  .then(() => {
    app.listen(PORT, () => console.log(`Lesson Engine running on port ${PORT}`));
  })
  .catch(error => {
    console.error('Startup failed:', error);
    process.exit(1);
  });

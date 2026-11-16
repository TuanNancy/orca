// Lane harness only, never merged: measures the #42783 expansion, the detector and the fix at scale.
import { execFileSync } from 'node:child_process'
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { expect, it } from 'vitest'
import type Database from '../../../sqlite/sync-database'
import { openJournalDatabase, runJournalTransaction } from './journal-database'
import {
  initializeJournalDomainDatabase,
  journalDomainDatabasePath,
  openJournalDomainDatabase
} from './journal-domain-database'
import { journalDatabasePath } from './journal-host-database'
import { insertJournalRow, publishJournalSessionEpoch } from '../durable-rows/journal-row-table'
import { journalSqlRetryRows } from '../journal-sql-retry-rows.test-fixture'
import { JournalSqlReaderPool } from '../journal-sql-reader-pool'
import { JournalSqlConversions } from '../journal-sql-conversions'
import { journalSqlReaderPath } from '../journal-sql-worker-bundle.test-fixture'
import { applyJournalSqlMaintenance } from '../journal-sql-maintenance'
import { reconcileJournalLegacySource as reconcileEc4f } from './journal-legacy-reconcile-ec4f.harness'
import { reconcileJournalLegacySource as reconcileFix } from './journal-legacy-reconcile'

const OUT = process.env.HARNESS_OUT ?? join(tmpdir(), 'journal-legacy-expansion-harness')
const DETECTOR = join(fileURLToPath(new URL('.', import.meta.url)), 'detect-migration-expansion.harness.mjs')
const rows = journalSqlRetryRows(false)

function legacyProfile(root: string, chats: number, tabbedEvery: number): Database.Database {
  const { db: original } = openJournalDatabase(journalDatabasePath(root))
  const now = Date.now()
  runJournalTransaction(original, () => {
    for (let index = 0; index < chats; index += 1) {
      const sessionId = `chat-${index}`
      publishJournalSessionEpoch(original, { sessionId, workspaceId: 'workspace' }, 'epoch')
      rows.forEach((row) => insertJournalRow(original, sessionId, row, now))
      original
        .prepare('INSERT INTO agent_session_records VALUES(?,?)')
        .run(sessionId, JSON.stringify({ sessionId, lease: { sessionId } }))
      if (index % tabbedEvery === 0) {
        original
          .prepare('INSERT INTO agent_session_tabs VALUES(?,?,?)')
          .run(`tab-${sessionId}`, sessionId, index)
      }
    }
    original
      .prepare("INSERT OR REPLACE INTO agent_session_store_meta VALUES('session_tabs_recorded','1')")
      .run()
  })
  return original
}

async function convert(root: string, ids: string[]): Promise<void> {
  const filename = journalDomainDatabasePath(root)
  const { db } = openJournalDomainDatabase(filename)
  const pool = new JournalSqlReaderPool(filename, journalDatabasePath(root), journalSqlReaderPath())
  const conversions = new JournalSqlConversions(
    {
      maintain: async (operation) =>
        runJournalTransaction(db, () => applyJournalSqlMaintenance(db, operation))
    },
    pool,
    async () => {}
  )
  for (const id of ids) {
    expect(await conversions.ensure(id)).toBe('epoch')
  }
  await conversions.close()
  await pool.close()
  db.close()
}

/** As a build from before revisions were kept left the file. */
function asOlderChatStore(db: Database.Database): void {
  db.exec(`DELETE FROM agent_session_store_meta
    WHERE key LIKE 'journal.legacy-revision%' OR key = 'journal.legacy-revisions-kept.v1'`)
  db.prepare(`UPDATE agent_session_store_meta SET value=json_remove(value,'$.wal.ino','$.wal.mtimeNs')
    WHERE key='journal.legacy-source-copy.v1'`).run()
}

const ids = (db: Database.Database, sql: string): string[] =>
  db
    .prepare(sql)
    .all()
    .map((row) => String(Object.values(row)[0]))

function snapshot(root: string): string {
  const dir = mkdtempSync(join(OUT, 'snap-'))
  for (const suffix of ['', '-wal']) {
    for (const file of [journalDatabasePath(root), journalDomainDatabasePath(root)]) {
      if (existsSync(file + suffix)) {
        copyFileSync(file + suffix, join(dir, file.split('/').at(-1)! + suffix))
      }
    }
  }
  return dir
}

function detect(dir: string): Record<string, any> {
  const legacy = join(dir, journalDatabasePath('/x').split('/').at(-1)!)
  const domain = join(dir, journalDomainDatabasePath('/x').split('/').at(-1)!)
  return JSON.parse(execFileSync(process.execPath, [DETECTOR, legacy, domain], { encoding: 'utf8' }))
}

function timed<T>(run: () => T): [T, number] {
  const start = performance.now()
  const value = run()
  return [value, Math.round((performance.now() - start) * 10) / 10]
}

it('expansion by ec4f7c6d86f, detection, and the fix at scale', { timeout: 1_800_000 }, async () => {
  mkdirSync(OUT, { recursive: true })
  const report: Record<string, unknown> = {}
  const root = mkdtempSync(join(OUT, 'profile-'))
  const chats = Number(process.env.HARNESS_CHATS ?? 2000)
  const original = legacyProfile(root, chats, 5)
  await initializeJournalDomainDatabase(root)
  // The user converts 10% of chats in the new build, closes some legacy tabs, opens a few others.
  const converted = Array.from({ length: chats / 10 }, (_, index) => `chat-${index * 10 + 3}`)
  await convert(root, converted)
  {
    const { db } = openJournalDomainDatabase(journalDomainDatabasePath(root))
    db.prepare("DELETE FROM agent_session_tabs WHERE session_id IN ('chat-0','chat-5','chat-10')").run()
    const tail = Number(db.prepare('SELECT MAX(position) AS p FROM agent_session_tabs').get()!.p)
    ;['chat-1', 'chat-2'].forEach((id, index) =>
      db.prepare('INSERT INTO agent_session_tabs VALUES(?,?,?)').run(`user-${id}`, id, tail + 1 + index)
    )
    asOlderChatStore(db)
    db.close()
  }
  const before = openJournalDomainDatabase(journalDomainDatabasePath(root)).db
  const tabsBefore = new Set(ids(before, 'SELECT session_id FROM agent_session_tabs'))
  const recordsBefore = new Set(ids(before, 'SELECT session_id FROM agent_session_records'))
  // Case (a): the fix on a pre-#42783 store copy: nothing opens, nothing is copied.
  const fixDir = snapshot(root)
  {
    const probeRoot = mkdtempSync(join(OUT, 'fix-a-'))
    for (const file of [journalDatabasePath(root), journalDomainDatabasePath(root)]) {
      for (const suffix of ['', '-wal']) {
        if (existsSync(file + suffix)) {
          copyFileSync(file + suffix, join(probeRoot, file.split('/').at(-1)! + suffix))
        }
      }
    }
    // The copy's original is closed, so its receipt reads as changed: the worst case for the fix.
    const { db } = openJournalDomainDatabase(journalDomainDatabasePath(probeRoot))
    const [, firstMs] = timed(() => reconcileFix(probeRoot, db))
    report.fixOnOlderStore = {
      firstPassMs: firstMs,
      tabs: ids(db, 'SELECT session_id FROM agent_session_tabs').length,
      tabsBefore: tabsBefore.size,
      records: ids(db, 'SELECT session_id FROM agent_session_records').length,
      recordsBefore: recordsBefore.size
    }
    const [, secondMs] = timed(() => reconcileFix(probeRoot, db))
    ;(report.fixOnOlderStore as Record<string, unknown>).unchangedLaunchMs = secondMs
    db.close()
  }
  report.fixDirSnapshot = fixDir
  // The faulty pass, as ec4f7c6d86f ran it.
  const [, ec4fMs] = timed(() => reconcileEc4f(root, before))
  const tabsAfter = ids(before, 'SELECT session_id FROM agent_session_tabs')
  const recordsAfter = ids(before, 'SELECT session_id FROM agent_session_records')
  const addedTabs = tabsAfter.filter((id) => !tabsBefore.has(id))
  const addedRecords = recordsAfter.filter((id) => !recordsBefore.has(id))
  report.ec4f = {
    ms: ec4fMs,
    tabsBefore: tabsBefore.size,
    tabsAfter: tabsAfter.length,
    addedTabs: addedTabs.length,
    addedRecords: addedRecords.length,
    addedCopyTabs: addedTabs.filter((id) => !id.startsWith('chat-')).length
  }
  report.detectAfterEc4f = detect(snapshot(root))

  // Later user actions: close 25 migration tabs, write into 10 migration-opened chats, open 3 new.
  const closed = addedTabs.slice(0, 25)
  for (const id of closed) {
    before.prepare('DELETE FROM agent_session_tabs WHERE session_id=?').run(id)
  }
  const written = addedTabs.slice(100, 110)
  for (const id of written) {
    before
      .prepare(`INSERT INTO journal_chat_state(session_id,workspace_id,epoch,last_sequence) VALUES(?,'workspace','epoch',99999)
        ON CONFLICT(session_id) DO UPDATE SET epoch='epoch', last_sequence=last_sequence+99999`)
      .run(id)
  }
  const tail = Number(before.prepare('SELECT MAX(position) AS p FROM agent_session_tabs').get()!.p)
  for (let index = 0; index < 3; index += 1) {
    before
      .prepare('INSERT INTO agent_session_records VALUES(?,?)')
      .run(`new-${index}`, JSON.stringify({ sessionId: `new-${index}` }))
    before.prepare('INSERT INTO agent_session_tabs VALUES(?,?,?)').run(`t-new-${index}`, `new-${index}`, tail + 1 + index)
  }
  report.userActions = { closedMigrationTabs: closed.length, wroteInMigrationTabs: written.length, newTabs: 3 }
  report.detectAfterUser = detect(snapshot(root))

  // Case (b): the fix on the expanded store changes nothing.
  const tabsExpanded = ids(before, 'SELECT tab_id FROM agent_session_tabs ORDER BY tab_id').join()
  const recordsExpanded = ids(before, 'SELECT session_id FROM agent_session_records ORDER BY 1').join()
  const [, fixExpandedMs] = timed(() => reconcileFix(root, before))
  report.fixOnExpanded = {
    ms: fixExpandedMs,
    tabsUnchanged: ids(before, 'SELECT tab_id FROM agent_session_tabs ORDER BY tab_id').join() === tabsExpanded,
    recordsUnchanged:
      ids(before, 'SELECT session_id FROM agent_session_records ORDER BY 1').join() === recordsExpanded
  }
  before.close()
  original.close()

  // Case (d): an unchanged launch does not scale with chat count.
  const unchanged: Record<string, number> = {}
  for (const count of [500, 5000]) {
    const scaleRoot = mkdtempSync(join(OUT, `scale-${count}-`))
    const scaleOriginal = legacyProfile(scaleRoot, count, 5)
    await initializeJournalDomainDatabase(scaleRoot)
    const { db } = openJournalDomainDatabase(journalDomainDatabasePath(scaleRoot))
    reconcileFix(scaleRoot, db)
    const samples = Array.from({ length: 20 }, () => timed(() => reconcileFix(scaleRoot, db))[1])
    unchanged[`chats${count}MedianMs`] = samples.toSorted((a, b) => a - b)[10]!
    db.close()
    scaleOriginal.close()
    rmSync(scaleRoot, { recursive: true, force: true })
  }
  report.unchangedLaunch = unchanged
  writeFileSync(join(OUT, 'report.json'), JSON.stringify(report, null, 2))
  console.log('HARNESS-REPORT', JSON.stringify(report, null, 2))
})

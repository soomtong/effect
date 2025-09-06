import * as PgKysely from "@effect/sql-kysely/Pg"
import { assert, describe, it } from "@effect/vitest"
import { Context, Effect, Layer } from "effect"
import type { Generated } from "kysely"
import { PgContainer } from "./utils.js"

export interface User {
  id: Generated<number>
  name: string
  nickname: string | null
}

interface Database {
  users: User
}

class PgDB extends Context.Tag("PgDB")<PgDB, PgKysely.EffectKysely<Database>>() {}

const PgLive = Layer.effect(PgDB, PgKysely.make<Database>()).pipe(Layer.provide(PgContainer.ClientLive))

describe("PgKysely", () => {
  it.effect("queries", () =>
    Effect.gen(function*() {
      const db = yield* PgDB
      yield* db.schema
        .createTable("users")
        .addColumn("id", "serial", (c) => c.primaryKey())
        .addColumn("name", "text", (c) => c.notNull())
        .addColumn("nickname", "text")

      const inserted = yield* db.insertInto("users").values({ name: "Alice" }).returningAll()
      const selected = yield* db.selectFrom("users").selectAll()
      const insertedMore = yield* db.insertInto("users").values({ name: "Queen" }).returningAll()
      const selectedMore = yield* db.selectFrom("users").selectAll()
      const selectedOne = yield* db.selectFrom("users").selectAll().where("id", "=", 1)
      const selectedFirst = yield* db.selectFrom("users").selectAll().limit(1).orderBy("id", "asc")
      const selectedLast = yield* db.selectFrom("users").selectAll().limit(1).orderBy("id", "desc")
      yield* db.updateTable("users").set({ name: "Bob", nickname: "The Bobinator" }).where("id", "=", 1)
      const selectedOneAfterUpdate = yield* db.selectFrom("users").selectAll().where("id", "=", 1)
      yield* db.deleteFrom("users").where("id", "=", 1)
      const selectedOneAfterDelete = yield* db.selectFrom("users").selectAll().where("id", "=", 1)
      const deleted = yield* db.selectFrom("users").selectAll()

      assert.deepStrictEqual(inserted, [{ id: 1, name: "Alice", nickname: null }])
      assert.deepStrictEqual(selected, [{ id: 1, name: "Alice", nickname: null }])
      assert.deepStrictEqual(insertedMore, [{ id: 2, name: "Queen", nickname: null }])
      assert.deepStrictEqual(selectedMore, [
        { id: 1, name: "Alice", nickname: null },
        { id: 2, name: "Queen", nickname: null }
      ])
      assert.deepStrictEqual(selectedOne, [{ id: 1, name: "Alice", nickname: null }])
      assert.deepStrictEqual(selectedFirst, [{ id: 1, name: "Alice", nickname: null }])
      assert.deepStrictEqual(selectedLast, [{ id: 2, name: "Queen", nickname: null }])
      assert.deepStrictEqual(selectedOneAfterUpdate, [{ id: 1, name: "Bob", nickname: "The Bobinator" }])
      assert.deepStrictEqual(selectedOneAfterDelete, [])
      assert.deepStrictEqual(deleted, [{ id: 2, name: "Queen", nickname: null }])
    }).pipe(Effect.provide(PgLive)), { timeout: 60000 })
})

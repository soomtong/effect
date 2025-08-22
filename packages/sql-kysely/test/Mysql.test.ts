import * as MysqlKysely from "@effect/sql-kysely/Mysql"
import { assert, describe, it } from "@effect/vitest"
import { Context, Effect, Layer } from "effect"
import type { Generated } from "kysely"
import { MysqlContainer } from "./utils.js"

export interface User {
  id: Generated<number>
  name: string
  nickname: string | null
}

interface Database {
  users: User
}

class MysqlDB extends Context.Tag("MysqlDB")<MysqlDB, MysqlKysely.EffectKysely<Database>>() {}

const MysqlLive = Layer.effect(MysqlDB, MysqlKysely.make<Database>()).pipe(Layer.provide(MysqlContainer.ClientLive))

describe("MysqlKysely", () => {
  it.effect("queries", () =>
    Effect.gen(function*() {
      const db = yield* MysqlDB

      yield* db.schema
        .createTable("users")
        .addColumn("id", "serial", (c) => c.primaryKey())
        .addColumn("name", "text", (c) => c.notNull())
        .addColumn("nickname", "text")

      yield* db.insertInto("users").values({ name: "Alice" })
      const inserted = yield* db.selectFrom("users").selectAll()
      yield* db.insertInto("users").values({ name: "Queen" })
      const insertedMore = yield* db.selectFrom("users").selectAll()
      const selectedOne = yield* db.selectFrom("users").selectAll().where("id", "=", 1)
      const selectedFirst = yield* db.selectFrom("users").selectAll().limit(1).orderBy("id", "asc")
      const selectedLast = yield* db.selectFrom("users").selectAll().limit(1).orderBy("id", "desc")
      yield* db.updateTable("users").set({ name: "Bob", nickname: "The Bobinator" }).where("id", "=", 1)
      const selectedOneAfterUpdate = yield* db.selectFrom("users").selectAll().where("id", "=", 1)
      yield* db.deleteFrom("users").where("id", "=", 1)
      const selectedOneAfterDelete = yield* db.selectFrom("users").selectAll().where("id", "=", 1)
      const deleted = yield* db.selectFrom("users").selectAll()

      assert.deepStrictEqual(inserted, [{ id: 1, name: "Alice", nickname: null }])
      assert.deepStrictEqual(insertedMore, [
        { id: 1, name: "Alice", nickname: null },
        { id: 2, name: "Queen", nickname: null }
      ])
      assert.deepStrictEqual(selectedOne, [{ id: 1, name: "Alice", nickname: null }])
      assert.deepStrictEqual(selectedFirst, [{ id: 1, name: "Alice", nickname: null }])
      assert.deepStrictEqual(selectedLast, [{ id: 2, name: "Queen", nickname: null }])
      assert.deepStrictEqual(selectedOneAfterUpdate, [{ id: 1, name: "Bob", nickname: "The Bobinator" }])
      assert.deepStrictEqual(selectedOneAfterDelete, [])
      assert.deepStrictEqual(deleted, [])
    }).pipe(Effect.provide(MysqlLive)), { timeout: 120000 })
})

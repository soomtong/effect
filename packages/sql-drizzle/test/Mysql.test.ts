import { SqlClient } from "@effect/sql"
import * as Mysql from "@effect/sql-drizzle/Mysql"
import { assert, describe, it } from "@effect/vitest"
import { desc } from "drizzle-orm"
import * as D from "drizzle-orm/mysql-core"
import { Effect, Layer } from "effect"
import { MysqlContainer } from "./utils-mysql.js"
import { DrizzleMysqlLive } from "./utils.js"

const users = D.mysqlTable("users", {
  id: D.serial("id").primaryKey(),
  name: D.text("name").notNull(),
  snakeCase: D.text("snake_case").notNull()
})

class ORM extends Effect.Service<ORM>()("ORM", { effect: Mysql.make({ schema: { users } }) }) {
  static Client = this.Default.pipe(Layer.provideMerge(MysqlContainer.ClientLive))
}

describe.sequential("Mysql", () => {
  it.effect("works", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const db = yield* Mysql.MysqlDrizzle

      yield* sql`CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, snake_case TEXT NOT NULL)`
      const returningId = yield* db.insert(users).values({ name: "Alice", snakeCase: "alice" }).$returningId()
      const results = yield* db.select().from(users)
      assert.deepStrictEqual(returningId, [{ id: 1 }])
      assert.deepStrictEqual(results, [{ id: 1, name: "Alice", snakeCase: "alice" }])
      yield* db.insert(users).values({ name: "Queen", snakeCase: "queen" })
      const results2 = yield* db.select().from(users).orderBy(desc(users.id)).limit(1)
      assert.deepStrictEqual(results2, [{ id: 2, name: "Queen", snakeCase: "queen" }])
    }).pipe(
      Effect.provide(DrizzleMysqlLive),
      Effect.catchTag("ContainerError", () => Effect.void)
    ), { timeout: 60000 })

  it.effect("transaction", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const db = yield* Mysql.MysqlDrizzle

      yield* sql`CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, snake_case TEXT NOT NULL)`

      yield* sql.withTransaction(Effect.gen(function*() {
        const returningId = yield* db.insert(users).values({ name: "Alice", snakeCase: "alice" }).$returningId()
        const results = yield* db.select().from(users)
        assert.deepStrictEqual(returningId, [{ id: 1 }])
        assert.deepStrictEqual(results, [{ id: 1, name: "Alice", snakeCase: "alice" }])
        return yield* Effect.fail("rollback")
      })).pipe(Effect.ignore)

      const results = yield* db.select().from(users)
      assert.deepStrictEqual(results, [])
    }).pipe(
      Effect.provide(DrizzleMysqlLive),
      Effect.catchTag("ContainerError", () => Effect.void)
    ), { timeout: 60000 })

  it.effect("orm", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const db = yield* ORM

      yield* sql`CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, snake_case TEXT NOT NULL)`
      yield* db.insert(users).values({ name: "Alice", snakeCase: "alice" })
      const results = yield* db.query.users.findMany({ columns: { id: true, name: false, snakeCase: true } })
      assert.deepStrictEqual(results, [{ id: 1, snakeCase: "alice" }])
      yield* db.insert(users).values({ name: "Queen", snakeCase: "queen" })
      const results2 = yield* db.query.users.findFirst({
        columns: { id: true, name: false, snakeCase: true },
        orderBy: [desc(users.id)]
      })
      assert.deepStrictEqual(results2, { id: 2, snakeCase: "queen" })
    }).pipe(
      Effect.provide(ORM.Client),
      Effect.catchTag("ContainerError", () => Effect.void)
    ), { timeout: 60000 })

  it.effect("remote callback", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const db = yield* Mysql.MysqlDrizzle
      yield* sql`CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, snake_case TEXT NOT NULL)`
      const rows = yield* Effect.promise(() =>
        db.insert(users).values({ name: "Alice", snakeCase: "alice" }).$returningId()
      )
      const results = yield* Effect.promise(() => db.select().from(users))
      assert.deepStrictEqual(results, [{ id: 1, name: "Alice", snakeCase: "alice" }])
      assert.deepStrictEqual(rows, [{ id: 1 }])
    }).pipe(
      Effect.provide(DrizzleMysqlLive),
      Effect.catchTag("ContainerError", () => Effect.void)
    ), { timeout: 60000 })

  it.effect("remote callback multiple values", () =>
    Effect.gen(function*() {
      const sql = yield* SqlClient.SqlClient
      const db = yield* Mysql.MysqlDrizzle
      yield* sql`CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL, snake_case TEXT NOT NULL)`
      const rows = yield* Effect.promise(() =>
        db.insert(users).values([
          { name: "Alice", snakeCase: "alice" },
          { name: "Bob", snakeCase: "bob" }
        ]).$returningId()
      )
      const results = yield* Effect.promise(() => db.select().from(users))
      assert.deepStrictEqual(results, [
        { id: 1, name: "Alice", snakeCase: "alice" },
        { id: 2, name: "Bob", snakeCase: "bob" }
      ])
      assert.deepStrictEqual(rows, [{ id: 1 }, { id: 2 }])
    }).pipe(
      Effect.provide(DrizzleMysqlLive),
      Effect.catchTag("ContainerError", () => Effect.void)
    ), { timeout: 60000 })
})

package com.nexledger

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import scala.concurrent.duration._
import java.util.UUID
import java.io.{File, PrintWriter}

/**
 * Warmup simulation: seeds accounts and writes their IDs to account_ids.csv
 * Run this before LedgerSimulation to pre-populate the feeder file.
 */
class WarmupSimulation extends Simulation {

  val baseUrl = sys.env.getOrElse("LEDGER_URL", "http://localhost:8080")

  val httpProtocol = http
    .baseUrl(baseUrl)
    .acceptHeader("application/json")
    .contentTypeHeader("application/json")

  val idempotencyFeeder = Iterator.continually(Map(
    "idempotencyKey" -> UUID.randomUUID().toString,
    "ownerId" -> s"warmup-${UUID.randomUUID().toString.take(8)}",
    "amount" -> (scala.util.Random.nextInt(500) + 100)
  ))

  val createAccounts = scenario("Seed Accounts")
    .feed(idempotencyFeeder)
    .exec(
      http("Create seed account")
        .post("/api/v1/accounts")
        .header("Idempotency-Key", "#{idempotencyKey}")
        .body(StringBody("""{"ownerId":"#{ownerId}","currency":"USD","initialBalance":50000.00}"""))
        .check(status.is(201))
        .check(jsonPath("$.accountId").saveAs("accountId"))
    )
    .exec { session =>
      session("accountId").asOption[String].foreach(id => out.synchronized(out.println(id)))
      session
    }

  // LedgerSimulation reads these from the classpath (src/test/resources/account_ids.csv)
  val csvFile = new File("src/test/resources/account_ids.csv")
  csvFile.getParentFile.mkdirs()
  val out = new PrintWriter(csvFile)
  out.println("accountId")

  after {
    out.close()
  }

  setUp(
    createAccounts.inject(
      rampUsers(500).during(10.seconds)
    )
  ).protocols(httpProtocol)
}

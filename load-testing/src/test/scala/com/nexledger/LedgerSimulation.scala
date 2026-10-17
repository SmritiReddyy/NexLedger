package com.nexledger

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import scala.concurrent.duration._
import java.util.UUID

/**
 * Mixed read/write load against the full stack.
 *
 * Checks are strict: a debit passes only with 200 (or 403, a deliberate fraud
 * block); a credit only with 200. Conflicts, lock timeouts and 5xx all count as
 * failures, so the success rate means what it says.
 *
 * RATE_SCALE multiplies every arrival rate (default 1.0 = ~300 req/s at peak),
 * so the same mix can be stepped up to find the sustainable rate.
 */
class LedgerSimulation extends Simulation {

  val baseUrl = sys.env.getOrElse("LEDGER_URL", "http://localhost:8080")
  val scale = sys.env.getOrElse("RATE_SCALE", "1.0").toDouble
  val steady = sys.env.getOrElse("STEADY_SECONDS", "120").toInt.seconds
  def rate(base: Double): Double = base * scale

  val httpProtocol = http
    .baseUrl(baseUrl)
    .acceptHeader("application/json")
    .contentTypeHeader("application/json")
    .shareConnections

  // Feeder: unique idempotency keys per request
  val idempotencyFeeder = Iterator.continually(Map(
    "idempotencyKey" -> UUID.randomUUID().toString,
    "ownerId" -> s"owner-${UUID.randomUUID().toString.take(8)}"
  ))

  // Pre-seeded account IDs, written by WarmupSimulation
  val accountIdFeeder = csv("account_ids.csv").circular

  // Amount feeder — must be defined before scenarios that reference #{amount}
  val amountFeeder = Iterator.continually(Map(
    "amount" -> (scala.util.Random.nextInt(999) + 1)
  ))

  // --- Scenarios ---

  val createAccountScenario = scenario("Create Account")
    .feed(idempotencyFeeder)
    .exec(
      http("POST /accounts")
        .post("/api/v1/accounts")
        .header("Idempotency-Key", "#{idempotencyKey}")
        .body(StringBody(
          """{"ownerId":"#{ownerId}","currency":"USD","initialBalance":100000.00}"""
        ))
        .check(status.is(201))
        .check(jsonPath("$.accountId").saveAs("newAccountId"))
    )

  val debitScenario = scenario("Debit Account")
    .feed(idempotencyFeeder)
    .feed(accountIdFeeder)
    .feed(amountFeeder)
    .exec(
      http("POST /accounts/:id/debit")
        .post("/api/v1/accounts/#{accountId}/debit")
        .header("Idempotency-Key", "#{idempotencyKey}")
        .body(StringBody(
          """{"amount":#{amount},"currency":"USD","reference":"LOAD-TEST"}"""
        ))
        .check(status.in(200, 403))
    )

  val creditScenario = scenario("Credit Account")
    .feed(idempotencyFeeder)
    .feed(accountIdFeeder)
    .feed(amountFeeder)
    .exec(
      http("POST /accounts/:id/credit")
        .post("/api/v1/accounts/#{accountId}/credit")
        .header("Idempotency-Key", "#{idempotencyKey}")
        .body(StringBody(
          """{"amount":#{amount},"currency":"USD","reference":"LOAD-TEST"}"""
        ))
        .check(status.is(200))
    )

  val getAccountScenario = scenario("Get Account")
    .feed(accountIdFeeder)
    .exec(
      http("GET /accounts/:id")
        .get("/api/v1/accounts/#{accountId}")
        .check(status.is(200))
    )

  val balanceScenario = scenario("Replay Balance")
    .feed(accountIdFeeder)
    .exec(
      http("GET /accounts/:id/balance")
        .get("/api/v1/accounts/#{accountId}/balance")
        .check(status.is(200))
    )

  val auditScenario = scenario("Get Audit Log")
    .feed(accountIdFeeder)
    .exec(
      http("GET /audit-log/:id")
        .get("/api/v1/audit-log/#{accountId}")
        .check(status.is(200))
    )

  def profile(base: Double) = Seq(
    rampUsersPerSec(1).to(rate(base)).during(30.seconds),
    constantUsersPerSec(rate(base)).during(steady)
  )

  setUp(
    createAccountScenario.inject(profile(5)),
    debitScenario.inject(profile(60)),
    creditScenario.inject(profile(60)),
    getAccountScenario.inject(profile(120)),
    balanceScenario.inject(profile(30)),
    auditScenario.inject(profile(25))
  ).protocols(httpProtocol)
   .assertions(
     global.successfulRequests.percent.gt(99)
   )
}

package com.nexledger

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import scala.concurrent.duration._
import java.util.UUID

class LedgerSimulation extends Simulation {

  val baseUrl = sys.env.getOrElse("LEDGER_URL", "http://localhost:8080")

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

  // Pre-seeded account IDs from warm-up
  val accountIdFeeder = csv("account_ids.csv").circular

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
    .exec(
      http("POST /accounts/:id/debit")
        .post("/api/v1/accounts/#{accountId}/debit")
        .header("Idempotency-Key", "#{idempotencyKey}")
        .body(StringBody(
          """{"amount":#{amount},"currency":"USD","reference":"LOAD-TEST"}"""
        ))
        .check(status.in(200, 409, 422, 403))
    )

  val creditScenario = scenario("Credit Account")
    .feed(idempotencyFeeder)
    .feed(accountIdFeeder)
    .exec(
      http("POST /accounts/:id/credit")
        .post("/api/v1/accounts/#{accountId}/credit")
        .header("Idempotency-Key", "#{idempotencyKey}")
        .body(StringBody(
          """{"amount":#{amount},"currency":"USD","reference":"LOAD-TEST"}"""
        ))
        .check(status.in(200, 409))
    )

  val getAccountScenario = scenario("Get Account")
    .feed(accountIdFeeder)
    .exec(
      http("GET /accounts/:id")
        .get("/api/v1/accounts/#{accountId}")
        .check(status.in(200, 404))
    )

  val auditScenario = scenario("Get Audit Log")
    .feed(accountIdFeeder)
    .exec(
      http("GET /audit-log/:id")
        .get("/api/v1/audit-log/#{accountId}")
        .check(status.in(200))
    )

  // --- Amount feeder ---
  val amountFeeder = Iterator.continually(Map(
    "amount" -> (scala.util.Random.nextInt(999) + 1)
  ))

  // --- Ramp-up simulation targeting 10K TPS ---
  setUp(
    createAccountScenario
      .inject(
        rampUsersPerSec(1).to(50).during(30.seconds),
        constantUsersPerSec(50).during(1.minute)
      ),
    debitScenario
      .feed(amountFeeder)
      .inject(
        nothingFor(15.seconds),
        rampUsersPerSec(10).to(500).during(2.minutes),
        constantUsersPerSec(500).during(3.minutes)
      ),
    creditScenario
      .feed(amountFeeder)
      .inject(
        nothingFor(15.seconds),
        rampUsersPerSec(10).to(400).during(2.minutes),
        constantUsersPerSec(400).during(3.minutes)
      ),
    getAccountScenario
      .inject(
        nothingFor(20.seconds),
        rampUsersPerSec(20).to(800).during(2.minutes),
        constantUsersPerSec(800).during(3.minutes)
      ),
    auditScenario
      .inject(
        nothingFor(30.seconds),
        rampUsersPerSec(5).to(100).during(1.minute),
        constantUsersPerSec(100).during(3.minutes)
      )
  ).protocols(httpProtocol)
   .assertions(
     global.responseTime.percentile(95).lt(200),
     global.successfulRequests.percent.gt(95),
     global.requestsPerSec.gt(1000)
   )
}

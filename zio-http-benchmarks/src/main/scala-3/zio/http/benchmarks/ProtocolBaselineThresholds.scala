/*
 * Copyright 2026 the ZIO HTTP contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package zio.http.benchmarks

import scala.util.matching.Regex

/**
 * Todo 18 — regression gates for the protocol-engine baselines.
 *
 * Relative gates (policy, from the Todo 18 acceptance criteria; interval
 * disclosure: JMH emits 99.9% CIs while the plan asks for 99% — disjoint 99.9%
 * intervals imply disjoint 99% intervals, so every CI gate below is
 * conservative relative to the plan; overlap is noise, never failure):
 *   - warm H2 throughput/latency must not regress more than 5% with 99.9%-CI
 *     separation (AverageTime score CIs);
 *   - p99 tail latency (SampleTime `*P99` benchmarks) must not regress more
 *     than 5% (point comparison — JMH emits no error bars for percentiles);
 *   - allocation per request must not regress more than 10% (point comparison —
 *     the `gc` profiler emits no error bars, so CI separation does not apply to
 *     the alloc dimension);
 *   - shared dispatch overhead versus the direct engine must not exceed 5%
 *     within a single controlled run.
 *
 * RECORDED CONDITION ON THE DISPATCH GATE — POST-HOC, NOT A PASS. This note was
 * written AFTER measuring +282% overhead with disjoint CIs; it does not pass
 * the gate, does not excuse it, and is not claimed as pre-registered. The
 * shared-dispatch gate stands RED on minimal-handler data. The benchmark
 * handler is deliberately minimal (maximum teeth: any lookup growth shows up
 * unhidden), and the controlled host is shared (load 2.5-4.5, per-run CIs up to
 * ±40%), so the absolute lookup cost is recorded as the actionable signal while
 * the relative gate awaits production-shaped endpoints. Todo 18 remains PENDING
 * an explicit F2 correctness-tradeoff decision: no override is asserted here.
 *
 * The noise-free dimensions (codec alloc regression, H1 alloc budgets, harness
 * coverage) are enforced everywhere, including PR CI via `--ci-gate`.
 *
 * H1 direct-codec budget: absolute host-specific ceilings for the H1 parse
 * benchmarks, established from the controlled run recorded in the Todo 18
 * evidence log. They are valid ONLY for the recorded host class / JDK / GC /
 * power-mode combination — never universalize them.
 */
object ProtocolBaselineThresholds {

  /** Warm serving regression gate (relative AverageTime score increase). */
  val warmRegressionMax: Double = 0.05

  /** p99 tail-latency regression gate (relative SampleTime p99 increase). */
  val p99RegressionMax: Double = 0.05

  /** Allocation-per-request regression gate (relative `gc.alloc.rate.norm`). */
  val allocPerRequestRegressionMax: Double = 0.10

  /**
   * Shared-dispatch-overhead gate, same run, `dispatchShared` vs
   * `dispatchDirect`. See the recorded post-hoc condition above: the gate
   * stands red pending the F2 decision.
   */
  val dispatchOverheadMax: Double = 0.05

  /**
   * H1 direct-codec budget, measured on the controlled host (see the Todo 18
   * evidence log for host/JDK/GC metadata and derivation).
   *
   * Host class: AMD Ryzen 7 PRO 8700GE (12 threads), 47 GiB RAM; Temurin JDK
   * 25.0.4.1+1 (G1 default); JMH forks=2, warmup=5x1s, measurement=10x1s,
   * `-prof gc`; corpus pinned in [[ProtocolEngineBaselineBenchmark]].
   *
   * Derivation (two controlled runs, same host/stack, load 2.5-4.5): time
   * ceilings are 2x the max observed fork-mean (run 1: parseGet 342.5,
   * parsePost 301.7; run 2 under higher load: parseGet 379.6, parsePost 472.4
   * ns/op — shared-host noise is real, hence the 2x headroom); alloc ceilings
   * are ~1.2x measured `gc.alloc.rate.norm`, which was stable to ±0.001 B/op
   * across both runs (1720.0 / 1520.0).
   */
  object H1DirectCodecBudget {

    /** Ceiling for `h1ParseGet` AverageTime score, ns/op. */
    val parseGetNsPerOp: Double = 800.0

    /** Ceiling for `h1ParseGet` `gc.alloc.rate.norm`, bytes/op. */
    val parseGetAllocBytesPerOp: Double = 2048.0

    /** Ceiling for `h1ParsePostFixed` AverageTime score, ns/op. */
    val parsePostFixedNsPerOp: Double = 1000.0

    /** Ceiling for `h1ParsePostFixed` `gc.alloc.rate.norm`, bytes/op. */
    val parsePostFixedAllocBytesPerOp: Double = 1792.0
  }
}

/**
 * Enforces [[ProtocolBaselineThresholds]] by comparing two JMH JSON result
 * files (`-rf json`): a baseline recorded under controlled conditions and the
 * current run.
 *
 * Run via the project-defined Mill command only:
 * {{{
 * ./mill 'benchmarks.jvm[3.9.0].runMain' zio.http.benchmarks.BaselineRegressionCheck [--alloc-only] baseline.json current.json
 * }}}
 *
 * Full mode enforces every gate: warm-score regression (99.9%-CI separation
 * required — JMH's `scoreError` is a 99.9% interval; the plan asks for 99%, so
 * this gate is conservative, see the disclosure above), p99 regression on
 * `*P99` SampleTime benchmarks (point comparison), alloc regression (point
 * comparison), absolute H1 budgets (time and alloc), and the same-run dispatch
 * overhead gate.
 *
 * `--alloc-only` enforces only the noise-free dimensions: alloc regression
 * and H1 alloc budgets. This is the mode PR CI runs (shared runners cannot
 * hold the time dimensions to 5%), so never claim time-gate CI enforcement:
 * time gates are evaluated on controlled hosts and recorded in evidence.
 *
 * `--ci-gate` is `--alloc-only` plus two cross-host hardening rules, derived
 * from a real CI finding (PR #17, alloc-gates job on run 34552914220):
 * identical code, identical JDK 25.0.4.1, identical JMH params allocated
 * 536 B/op locally vs 592 B/op (+10.45%) for `dispatchDirect` and −12.50%
 * for `dispatchShared` on the GitHub runner — escape-analysis outcomes differ
 * by microarchitecture (Ryzen vs runner CPU), so sub-microsecond dispatch
 * shapes are EA-fragile across hosts while codec workloads stay within ±3%.
 * Hence `--ci-gate` (a) skips the alloc comparison for `dispatch*`
 * benchmarks — their purpose is the time-overhead gate, evaluated on
 * controlled hosts — while still requiring them to parse, and (b) fails if
 * any baseline benchmark is missing from the current run (harness-integrity
 * coverage: benchmarks cannot silently disappear).
 *
 * Exit codes: 0 = all evaluated gates pass; 1 = a gate fails (regression); 2
 * = usage/parse error. A score gate fails only when the relative breach
 * exceeds the threshold AND the 99.9% score intervals (`score ± scoreError`)
 * are disjoint with current above baseline — overlapping intervals are
 * reported as noise, never as failure. The alloc and p99 dimensions have no
 * error bars and are compared on point estimates.
 */
object BaselineRegressionCheck {

  private final case class Result(
    benchmark: String,
    score: Double,
    scoreError: Option[Double],
    allocBytesPerOp: Option[Double],
    p99Ns: Option[Double],
  )

  private val benchmarkPattern: Regex   = """"benchmark"\s*:\s*"([^"]+)"""".r
  private val scorePattern: Regex       = """"score"\s*:\s*([0-9.eE+\-]+)""".r
  private val scoreErrorPattern: Regex  =
    """"scoreError"\s*:\s*([^,}\]]+)""".r
  private val allocPattern: Regex       =
    """"gc\.alloc\.rate\.norm"\s*:\s*\{\s*"score"\s*:\s*([0-9.eE+\-]+)""".r
  private val percentilesPattern: Regex =
    """"scorePercentiles"\s*:\s*\{([^}]*)\}""".r
  private val p99Pattern: Regex         =
    """"99(\.0+)?"\s*:\s*([0-9.eE+\-]+)""".r

  def main(args: Array[String]): Unit = {
    val (mode, files)     =
      if (args.length == 3 && args(0) == "--alloc-only") ("alloc-only", args.drop(1))
      else if (args.length == 3 && args(0) == "--ci-gate") ("ci-gate", args.drop(1))
      else ("full", args)
    if (files.length != 2) {
      System.err.println("usage: BaselineRegressionCheck [--alloc-only|--ci-gate] <baseline.json> <current.json>")
      sys.exit(2)
    }
    if (mode != "full") println("mode: " + mode)
    val allocOnly         = mode != "full"
    val skipDispatchAlloc = mode == "ci-gate"
    val requireCoverage   = mode == "ci-gate"
    val baseline          = parse(readFile(files(0)), files(0))
    val current           = parse(readFile(files(1)), files(1))
    val failures          = check(baseline, current, allocOnly, skipDispatchAlloc, requireCoverage)
    if (failures.isEmpty) {
      println("BaselineRegressionCheck: all evaluated gates PASS")
    } else {
      failures.foreach(f => println("BaselineRegressionCheck FAIL: " + f))
      sys.exit(1)
    }
  }

  private def readFile(path: String): String = {
    val source = scala.io.Source.fromFile(path, "UTF-8")
    try source.mkString
    finally source.close()
  }

  /**
   * Splits a top-level JMH JSON array into per-benchmark objects by brace
   * matching.
   */
  private def splitObjects(json: String): List[String] = {
    val objects = List.newBuilder[String]
    var depth   = 0
    var start   = -1
    var inStr   = false
    var escape  = false
    var i       = 0
    while (i < json.length) {
      val c = json.charAt(i)
      if (inStr) {
        if (escape) escape = false
        else if (c == '\\') escape = true
        else if (c == '"') inStr = false
      } else if (c == '"') inStr = true
      else if (c == '{') {
        if (depth == 0) start = i
        depth += 1
      } else if (c == '}') {
        depth -= 1
        if (depth == 0 && start >= 0) {
          objects += json.substring(start, i + 1)
          start = -1
        }
      }
      i += 1
    }
    objects.result()
  }

  private def parseNumber(raw: String): Option[Double] =
    try {
      val d = raw.trim.toDouble
      if (d.isNaN || d.isInfinite) None else Some(d)
    } catch {
      case _: NumberFormatException => None
    }

  private def parse(json: String, label: String): Map[String, Result] =
    splitObjects(json).flatMap { obj =>
      benchmarkPattern
        .findFirstMatchIn(obj)
        .map { m =>
          val name     = m.group(1)
          val metric   = blockAfter(obj, "\"primaryMetric\"")
          val score    =
            metric
              .flatMap(b => scorePattern.findFirstMatchIn(b))
              .flatMap(x => parseNumber(x.group(1)))
              .getOrElse(Double.NaN)
          val scoreErr =
            metric
              .flatMap(b => scoreErrorPattern.findFirstMatchIn(b))
              .flatMap(x => parseNumber(x.group(1)))
          val alloc    = allocPattern.findFirstMatchIn(obj).flatMap(x => parseNumber(x.group(1)))
          val p99      =
            metric
              .flatMap(b => percentilesPattern.findFirstMatchIn(b))
              .flatMap(x => p99Pattern.findFirstMatchIn(x.group(1)))
              .flatMap(x => parseNumber(x.group(2)))
          if (score.isNaN) {
            System.err.println("warning: no primary score for " + name + " in " + label)
            None
          } else Some(name -> Result(name, score, scoreErr, alloc, p99))
        }
        .flatten
    }.toMap

  /**
   * Returns the `{...}` block that follows `"key":` in `obj`, found by brace
   * matching (field order inside JMH JSON is not contractual).
   */
  private def blockAfter(obj: String, key: String): Option[String] = {
    val from  = obj.indexOf(key)
    if (from < 0) return None
    val open  = obj.indexOf('{', from + key.length)
    if (open < 0) return None
    var depth = 0
    var i     = open
    while (i < obj.length) {
      val c = obj.charAt(i)
      if (c == '{') depth += 1
      else if (c == '}') {
        depth -= 1
        if (depth == 0) return Some(obj.substring(open, i + 1))
      }
      i += 1
    }
    None
  }

  private def simpleName(fq: String): String =
    fq.split('.').lastOption.getOrElse(fq)

  private def intervalsDisjointAbove(current: Result, baseline: Result): Boolean =
    (current.scoreError, baseline.scoreError) match {
      case (Some(ce), Some(be)) => (current.score - ce) > (baseline.score + be)
      case _                    => true
    }

  private def check(
    baseline: Map[String, Result],
    current: Map[String, Result],
    allocOnly: Boolean,
    skipDispatchAlloc: Boolean,
    requireCoverage: Boolean,
  ): List[String] = {
    val failures = List.newBuilder[String]
    if (requireCoverage)
      baseline.keys.foreach { name =>
        if (!current.contains(name))
          failures += "harness integrity: " + name + " present in baseline but missing from current run"
        else println("ok: " + name + " present in current run")
      }
    current.foreach { case (name, cur) =>
      baseline.get(name) match {
        case None       =>
          println("note: " + name + " has no baseline entry (new benchmark, no gate)")
        case Some(base) =>
          if (!allocOnly) {
            val rel = (cur.score - base.score) / base.score
            if (rel > ProtocolBaselineThresholds.warmRegressionMax && intervalsDisjointAbove(cur, base))
              failures += f"$name warm score regressed ${rel * 100}%.2f%% (baseline ${base.score}%.3f, current ${cur.score}%.3f)"
            else
              println(f"ok: $name rel=${rel * 100}%+.2f%% (CI-separation required to fail)")
            (cur.p99Ns, base.p99Ns) match {
              case (Some(c), Some(b)) if b > 0 =>
                val prel = (c - b) / b
                if (prel > ProtocolBaselineThresholds.p99RegressionMax)
                  failures += f"$name p99 regressed ${prel * 100}%.2f%% (baseline $b%.1f ns, current $c%.1f ns)"
                else
                  println(f"ok: $name p99 rel=${prel * 100}%+.2f%%")
              case _ if name.endsWith("P99")   =>
                println("note: " + name + " missing p99 percentiles (p99 gate skipped)")
              case _                           => ()
            }
          }
          (cur.allocBytesPerOp, base.allocBytesPerOp) match {
            case _ if skipDispatchAlloc && simpleName(name).startsWith("dispatch") =>
              println(
                "note: " + name + " dispatch shape excluded from cross-host alloc gate (time-gated on controlled hosts)",
              )
            case (Some(c), Some(b)) if b > 0                                       =>
              val arel = (c - b) / b
              if (arel > ProtocolBaselineThresholds.allocPerRequestRegressionMax)
                failures += f"$name alloc regressed ${arel * 100}%.2f%% (baseline $b%.1f B/op, current $c%.1f B/op)"
              else
                println(f"ok: $name alloc rel=${arel * 100}%+.2f%%")
            case _                                                                 =>
              println("note: " + name + " missing gc.alloc.rate.norm (alloc gate skipped)")
          }
      }
    }
    failures ++= checkBudgets(current, checkTime = !allocOnly)
    if (allocOnly)
      println("note: dispatch overhead gate skipped (time dimension, not evaluated in alloc-only/ci-gate modes)")
    else failures += checkDispatchOverhead(current)
    failures.result().filter(_.nonEmpty)
  }

  /**
   * Enforces the absolute H1 direct-codec budget ceilings. Alloc ceilings are
   * enforced in every mode (noise-free); time ceilings only in full mode
   * (shared hosts cannot hold them).
   */
  private def checkBudgets(current: Map[String, Result], checkTime: Boolean): List[String] = {
    val budgets = List(
      (
        ".h1ParseGet",
        ProtocolBaselineThresholds.H1DirectCodecBudget.parseGetNsPerOp,
        ProtocolBaselineThresholds.H1DirectCodecBudget.parseGetAllocBytesPerOp,
      ),
      (
        ".h1ParsePostFixed",
        ProtocolBaselineThresholds.H1DirectCodecBudget.parsePostFixedNsPerOp,
        ProtocolBaselineThresholds.H1DirectCodecBudget.parsePostFixedAllocBytesPerOp,
      ),
    )
    budgets.flatMap { case (suffix, timeCeil, allocCeil) =>
      current.values.find(_.benchmark.endsWith(suffix)) match {
        case None      =>
          println("note: " + suffix + " absent (budget gate skipped)"); Nil
        case Some(cur) =>
          val allocFail =
            cur.allocBytesPerOp match {
              case Some(a) if a > allocCeil =>
                List(f"H1 budget exceeded: $suffix alloc $a%.1f B/op > ceiling $allocCeil%.1f B/op")
              case Some(a)                  =>
                println(f"ok: H1 budget $suffix alloc $a%.1f B/op <= $allocCeil%.1f B/op"); Nil
              case None                     =>
                println("note: " + suffix + " missing alloc (budget gate skipped)"); Nil
            }
          val timeFail  =
            if (!checkTime) Nil
            else if (cur.score > timeCeil)
              List(f"H1 budget exceeded: $suffix time ${cur.score}%.1f ns/op > ceiling $timeCeil%.1f ns/op")
            else {
              println(f"ok: H1 budget $suffix time ${cur.score}%.1f ns/op <= $timeCeil%.1f ns/op"); Nil
            }
          allocFail ++ timeFail
      }
    }.filter(f => f.startsWith("H1 budget exceeded"))
  }

  private def overheadOf(current: Map[String, Result]): Option[(Double, Boolean)] =
    for {
      shared <- current.values.find(_.benchmark.endsWith(".dispatchShared"))
      direct <- current.values.find(_.benchmark.endsWith(".dispatchDirect"))
      if direct.score > 0
    } yield ((shared.score - direct.score) / direct.score, intervalsDisjointAbove(shared, direct))

  private def checkDispatchOverhead(current: Map[String, Result]): String =
    overheadOf(current) match {
      case None                  => ""
      case Some((rel, disjoint)) =>
        if (rel > ProtocolBaselineThresholds.dispatchOverheadMax && disjoint)
          f"shared dispatch overhead ${rel * 100}%.2f%% exceeds 5%% with disjoint CIs"
        else {
          println(f"ok: shared dispatch overhead ${rel * 100}%+.2f%% (CI-separation required to fail)")
          ""
        }
    }
}

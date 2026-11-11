// The decision matrix of chapter 24.
//   rank [scenario]           eliminates the options below the minimum, ranks the rest
//   sensitivity [scenario]    shows which weight change would swap the winner
//   cost [messages] [kb]      estimates the monthly cost of SQS and MSK for that volume
// Without a scenario, it uses all of those in matrix.json.
import fs from "node:fs";

type Criterion = { description: string; evidence: string; scores: number[] };
type Scenario = {
  description: string;
  weights: Record<string, number>;
  eliminatory?: Record<string, number>;
};
type Cost = {
  source: string;
  hours_per_month: number;
  team_hourly_rate: number;
  sqs: { tiers: [number | null, number][]; free: number; requests_per_message: number;
    chunk_kb: number; team_hours: number };
  msk: { brokers: number; broker_hour: number; disk_gb_month: number;
    retention_days: number; replication: number; cross_zone_gb: number;
    team_hours: number };
};
type Matrix = {
  options: string[];
  criteria: Record<string, Criterion>;
  scenarios: Record<string, Scenario>;
  cost: Cost;
};

const matrix: Matrix = JSON.parse(fs.readFileSync("matrix.json", "utf8"));
const [command = "rank", name] = process.argv.slice(2);
const number = (value: number) => value.toLocaleString("en-US");

// ① Eliminatory criteria: an option below the minimum on one criterion is out of the
// race, however high its scores on the others
function eliminated(scenario: Scenario): Map<number, string> {
  const out = new Map<number, string>();
  for (const [criterion, minimum] of Object.entries(scenario.eliminatory ?? {})) {
    matrix.criteria[criterion].scores.forEach((score, i) => {
      if (score < minimum && !out.has(i)) {
        out.set(i, `${criterion} ${score} < ${minimum}`);
      }
    });
  }
  return out;
}

// ② Weighted score: the sum of score x weight, divided by the sum of weights (1 to 5)
type Position = { option: number; score: number };
function score(scenario: Scenario, weights = scenario.weights): Position[] {
  const out = eliminated(scenario);
  const total = Object.values(weights).reduce((a, b) => a + b, 0);
  return matrix.options
    .map((_, option) => ({
      option,
      score: Object.entries(weights).reduce((sum, [criterion, weight]) =>
        sum + weight * matrix.criteria[criterion].scores[option], 0) / total,
    }))
    .filter(({ option }) => !out.has(option))
    .sort((a, b) => b.score - a.score);
}

function rank(id: string, scenario: Scenario): void {
  console.log(`\n${id}: ${scenario.description}`);
  for (const [option, reason] of eliminated(scenario)) {
    console.log(`  eliminated ${matrix.options[option]} (${reason})`);
  }
  score(scenario).forEach(({ option, score }, i) => {
    console.log(`  ${`${i + 1}.`.padEnd(2)} ${score.toFixed(2).padStart(6)}  ` +
      matrix.options[option]);
  });
}

// ③ Sensitivity: for each criterion, the smallest weight change (from -2 to +2) that
// changes the first place. If no change does, the decision is stable.
function sensitivity(id: string, scenario: Scenario): void {
  const winner = score(scenario)[0]?.option;
  console.log(`\n${id}: ${matrix.options[winner]} wins`);
  let stable = true;
  for (const criterion of Object.keys(matrix.criteria)) {
    for (const change of [-1, 1, -2, 2]) {
      const weight = (scenario.weights[criterion] ?? 0) + change;
      if (weight < 0) continue;
      const weights = { ...scenario.weights, [criterion]: weight };
      const next = score(scenario, weights)[0]?.option;
      if (next !== winner) {
        console.log(`  weight of ${criterion} ${change > 0 ? "+" : ""}${change}: ` +
          `${matrix.options[next]} wins`);
        stable = false;
        break;
      }
    }
  }
  if (stable) console.log("  no change of up to 2 points swaps the winner");
}

// ④ Cost: SQS charges per request (each 64 KB chunk counts as one); MSK charges for
// the running brokers, used or not, plus disk and cross-zone traffic. The team's
// hours go into both, at the hourly rate in matrix.json.
const { cost: c } = matrix;
const usd = (value: number) => `$${number(Math.round(value))}`;

function sqsCost(messages: number, kb: number): number {
  let requests = messages * c.sqs.requests_per_message *
    Math.ceil(kb / c.sqs.chunk_kb) - c.sqs.free;
  let total = 0;
  let start = 0;
  for (const [end, price] of c.sqs.tiers) {
    const inThis = Math.min(Math.max(requests, 0), (end ?? Infinity) - start);
    total += (inThis / 1e6) * price;
    requests -= inThis;
    start = end ?? start;
  }
  return total;
}

function mskCost(messages: number, kb: number): number {
  const gb = (messages * kb) / 1e6;
  const brokers = c.msk.brokers * c.msk.broker_hour * c.hours_per_month;
  // Disk: the volume kept by the retention, times the replicas
  const disk = gb * (c.msk.retention_days / 30) * c.msk.replication *
    c.msk.disk_gb_month;
  // Clients: 2 of every 3 bytes produced and read (one group) cross zones
  const zones = gb * (2 / 3) * 2 * c.msk.cross_zone_gb;
  return brokers + disk + zones;
}

function cost(messages: number, kb: number): void {
  const teamSqs = c.sqs.team_hours * c.team_hourly_rate;
  const teamMsk = c.msk.team_hours * c.team_hourly_rate;
  const perSecond = (n: number) => number(Math.round(n / (30 * 86400)));
  console.log(`\nestimated monthly cost: ${number(messages)} messages of ` +
    `${kb} KB/month (about ${perSecond(messages)}/s)`);
  console.log(`  ${c.source}`);
  console.log(`  team at ${usd(c.team_hourly_rate)} per hour`);
  console.log(`  ${"".padEnd(16)}${"service".padStart(12)}${"team".padStart(12)}` +
    `${"total".padStart(12)}`);
  const line = (label: string, service: number, team: number) =>
    console.log(`  ${label.padEnd(16)}${usd(service).padStart(12)}` +
      `${usd(team).padStart(12)}${usd(service + team).padStart(12)}`);
  line("SQS Standard", sqsCost(messages, kb), teamSqs);
  line(`MSK (${c.msk.brokers} brokers)`, mskCost(messages, kb), teamMsk);
  // Crossover point: the first volume at which MSK costs less than SQS, searched in
  // 1% steps and refined to the million
  const difference = (n: number) =>
    mskCost(n, kb) + teamMsk - sqsCost(n, kb) - teamSqs;
  let low = 1e6;
  while (low < 1e12 && difference(low * 1.01) > 0) low *= 1.01;
  let high = low * 1.01;
  if (low >= 1e12) {
    console.log(`  with ${kb} KB, MSK does not get cheaper up to 1 trillion ` +
      "messages per month");
    return;
  }
  while (high - low > 1e6) {
    const middle = (low + high) / 2;
    if (difference(middle) > 0) low = middle;
    else high = middle;
  }
  console.log(`  the curves cross near ${number(Math.round(high / 1e6))}` +
    ` million messages/month (about ${perSecond(high)}/s)`);
}

if (command === "cost") {
  cost(Number(name ?? 10_000_000), Number(process.argv[4] ?? 1));
} else {
  const scenarios = name ? { [name]: matrix.scenarios[name] } : matrix.scenarios;
  for (const [id, scenario] of Object.entries(scenarios)) {
    if (command === "sensitivity") sensitivity(id, scenario);
    else rank(id, scenario);
  }
}

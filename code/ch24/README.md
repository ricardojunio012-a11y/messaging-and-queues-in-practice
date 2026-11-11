# Chapter 24: the decision matrix

- `matrix.json`: the options, the criteria (with the scores from 1 to 5 and the evidence
  for each score, taken from the book's labs), the scenarios (weights and eliminatory
  requirements) and, under `cost`, the prices and the assumptions of the cost estimate
  (checked on October 26, 2026; prices in US dollars)
- `node/src/matrix.ts`: ① eliminates, ② weighs, ③ tests the sensitivity and ④ estimates
  the cost

`matrix.json` is mounted in the container: edit it and run again, without rebuilding.

## Names used in this chapter

- Criteria: `durability`, `replay`, `ordering`, `routing`, `throughput`, `jobs`,
  `operations`, `infrastructure`
- Scenarios: `jobs`, `events`, `integration`
- Commands: `rank`, `sensitivity`, `cost`

## How to run

1. `docker compose run --rm matrix` (ranks every scenario)
2. `docker compose run --rm matrix sensitivity`
3. `docker compose run --rm matrix rank jobs` (a single scenario)
4. `docker compose run --rm matrix cost 10000000 10` (SQS against MSK for 10 million
   messages of 10 KB per month, with the team's hours and the crossover point)

The full walkthrough is in section 24.6 of the book.

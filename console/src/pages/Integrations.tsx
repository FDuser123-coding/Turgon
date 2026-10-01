import { api } from "../api";
import { Empty, ErrorBanner, Link, Pill } from "../components";
import { ago } from "../format";
import { usePoll } from "../hooks";
import type { Flow, FlowStep, IntegrationSystem, RunSummary } from "../types";

// Integrations is the map of what is connected to what: every flow from the
// event that starts it to the writes it makes, and the systems involved,
// with what is happening in each right now.
export function Integrations() {
  const map = usePoll(api.integrations, 30000);
  const runs = usePoll(api.runs);
  const systems = new Map((map.data?.systems ?? []).map((s) => [s.name, s]));
  const all = runs.data ?? [];
  const activity = (f: Flow) => all.filter((r) => r.workflow === f.name).length;
  const flows = (map.data?.flows ?? []).filter((f) => f.deployable).sort((a, b) => activity(b) - activity(a) || a.name.localeCompare(b.name));
  const blocked = (map.data?.flows ?? []).filter((f) => !f.deployable);
  const connected = (map.data?.systems ?? []).filter((s) => s.role !== "unused");
  const running = all.filter((r) => r.status === "running");

  return (
    <section>
      <header className="page-head">
        <h1>Integrations</h1>
        <p className="muted">
          What is connected to what. Each flow starts from an event in one system and writes to others only through
          Turgon's safeguards: the data is mapped and the customer matched, every write is tried as a dry run first,
          a person approves it where the policy says so, and if a later write fails the earlier ones are undone.
        </p>
      </header>
      <ErrorBanner error={map.error ?? map.data?.error ?? runs.error ?? null} />

      <div className="tiles">
        <Tile value={connected.length} label="systems connected" />
        <Tile value={flows.length} label="flows" />
        <Tile value={running.length} label="runs in progress" />
        <Tile value={all.filter((r) => r.pending).length} label="waiting for approval" tone="info" to="/approvals" />
        <Tile value={all.filter((r) => r.status === "failed").length} label="failed runs" tone="bad" to="/runs" />
      </div>

      {flows.length > 0 && <ConnectionMap flows={flows} runs={all} />}

      <h2>Flows</h2>
      {map.data && flows.length === 0 && (
        <Empty>No flows. Start the console with --catalog pointing at the catalog your specs are compiled from.</Empty>
      )}
      {flows.map((f) => (
        <FlowCard key={f.name} flow={f} systems={systems} runs={all.filter((r) => r.workflow === f.name)} />
      ))}

      <h2>Systems</h2>
      <div className="systems">
        {connected.map((s) => (
          <SystemCard key={s.name} system={s} />
        ))}
      </div>

      {blocked.length > 0 && (
        <details>
          <summary>
            {blocked.length} recipe{blocked.length > 1 ? "s" : ""} in the catalog cannot run on their own
          </summary>
          <ul className="findings">
            {blocked.map((f) => (
              <li key={f.name} className="sev-warning">
                <span className="mono">{f.name}</span>: {f.problem}
              </li>
            ))}
          </ul>
        </details>
      )}
    </section>
  );
}

function Tile({ value, label, tone, to }: { value: number; label: string; tone?: "info" | "bad"; to?: string }) {
  const body = (
    <>
      <span className={`tile-value${tone && value > 0 ? ` tone-${tone}` : ""}`}>{value}</span>
      <span className="tile-label">{label}</span>
    </>
  );
  return to ? (
    <Link to={to} className="tile">
      {body}
    </Link>
  ) : (
    <div className="tile">{body}</div>
  );
}

const delivery: Record<string, string> = {
  webhook: "pushed by webhook, polled to reconcile",
  polling: "polled",
  outbox: "read from an outbox table",
  "change capture": "read from the database log",
  subscription: "pushed over a subscription",
};

function FlowCard({ flow, systems, runs }: { flow: Flow; systems: Map<string, IntegrationSystem>; runs: RunSummary[] }) {
  const src = systems.get(flow.trigger.system);
  const waiting = runs.filter((r) => r.pending).length;
  const failed = runs.filter((r) => r.status === "failed").length;
  const last = runs.reduce<string | null>((a, r) => (a === null || r.started > a ? r.started : a), null);
  return (
    <article className="card flow" id={`flow-${flow.name}`}>
      <div className="card-head">
        <div>
          <span className="title mono">{flow.name}</span>{" "}
          <Pill tone="neutral">{flow.level}</Pill> {flow.slo && <Pill tone="neutral">{flow.slo}</Pill>}
          {flow.description && <p className="muted flow-desc">{flow.description}</p>}
        </div>
        <div className="flow-stats">
          {runs.length === 0 ? (
            <span className="muted small">no runs yet</span>
          ) : (
            <>
              <Link to={`/runs?flow=${encodeURIComponent(flow.name)}`} className="small">
                {runs.length} run{runs.length > 1 ? "s" : ""}
              </Link>
              {waiting > 0 && <Pill tone="info">{waiting} awaiting approval</Pill>}
              {failed > 0 && <Pill tone="bad">{failed} failed</Pill>}
              {last && <span className="muted small">last {ago(last)}</span>}
            </>
          )}
        </div>
      </div>
      <ol className="pipeline">
        <li className="node node-source">
          <span className="node-kind">When</span>
          <span className="node-system">{flow.trigger.system}</span>
          <span className="mono small">{flow.trigger.event}</span>
          <span className="muted small">{delivery[flow.trigger.delivery] ?? flow.trigger.delivery}</span>
          {src?.product && <span className="muted small">{src.product}</span>}
        </li>
        {fold(flow.steps).map((s, i) => (
          <Step key={i} step={s.step} mapped={s.mapped} source={flow.trigger.system} />
        ))}
      </ol>
    </article>
  );
}

// fold attaches each map step to the step after it: a mapping shapes the
// data that step uses.
function fold(steps: FlowStep[]): { step: FlowStep; mapped?: FlowStep }[] {
  const out: { step: FlowStep; mapped?: FlowStep }[] = [];
  let pending: FlowStep | undefined;
  for (const s of steps) {
    if (s.kind === "map") {
      pending = s;
      continue;
    }
    out.push({ step: s, mapped: pending });
    pending = undefined;
  }
  if (pending) out.push({ step: pending });
  return out;
}

function Mapped({ step }: { step?: FlowStep }) {
  if (!step) return null;
  return (
    <span className="muted small" title={`${step.from} → ${step.to}`}>
      mapped by <span className="mono">{step.mapping?.replace(/@.*/, "")}</span>
      {step.fields ? ` (${step.fields} fields)` : ""}
    </span>
  );
}

function Step({ step, mapped, source }: { step: FlowStep; mapped?: FlowStep; source: string }) {
  switch (step.kind) {
    case "map":
      return (
        <li className="node node-step">
          <span className="node-kind">Map</span>
          <Mapped step={step} />
        </li>
      );
    case "resolve":
      return (
        <li className="node node-step">
          <span className="node-kind">Match</span>
          <span className="small">{step.entity?.replace(/^model\./, "")} to its master record</span>
          <span className="muted small">{step.strategy} match</span>
          <Mapped step={mapped} />
        </li>
      );
    default:
      return (
        <li className={`node node-write${step.system === source ? " node-back" : ""}`}>
          <span className="node-kind">{step.system === source ? "Write back" : "Write"}</span>
          <span className="node-system">{step.system}</span>
          <span className="mono small">{step.operation}</span>
          <Mapped step={mapped} />
          <span className="guards">
            {step.risk && <Pill tone={step.risk === "high" ? "bad" : "warn"}>{step.risk} risk</Pill>}
            {step.simulation && <Pill tone="neutral">dry run</Pill>}
            {step.approval && step.approval !== "none" && (
              <Pill tone="info">{step.approval === "policy" ? "approval by policy" : `approval: ${step.approval}`}</Pill>
            )}
            {step.compensation && <Pill tone="neutral">undo: {step.compensation}</Pill>}
            {step.plugins?.map((p) => (
              <Pill key={p} tone="info">then plugin {p}</Pill>
            ))}
          </span>
        </li>
      );
  }
}

const roles: Record<string, string> = { source: "sends events", target: "receives writes", both: "sends events, receives writes" };

function SystemCard({ system }: { system: IntegrationSystem }) {
  return (
    <article className="card system">
      <div className="card-head">
        <span className="title mono">{system.name}</span>
        <Pill tone={system.role === "target" ? "warn" : system.role === "source" ? "ok" : "info"}>{roles[system.role]}</Pill>
      </div>
      {system.description && <p className="flow-desc">{system.description}</p>}
      <dl className="facts">
        {system.connector && (
          <>
            <dt>Connector</dt>
            <dd>
              <span className="mono">{system.connector}</span>
              {system.product && <span className="muted"> · {system.product}</span>}
            </dd>
          </>
        )}
        {system.events.length > 0 && (
          <>
            <dt>Events</dt>
            <dd>
              {system.events.map((e) => (
                <div key={e.name}>
                  <span className="mono">{e.name}</span> <span className="muted small">{delivery[e.delivery] ?? e.delivery}</span>
                </div>
              ))}
            </dd>
          </>
        )}
        {system.operations.length > 0 && (
          <>
            <dt>Writes</dt>
            <dd className="mono">{system.operations.join(", ")}</dd>
          </>
        )}
        <dt>Flows</dt>
        <dd>{system.flows.length}</dd>
      </dl>
    </article>
  );
}

// ConnectionMap draws every flow as a line from the system whose event
// starts it to each system it writes to, through Turgon in the middle.
function ConnectionMap({ flows, runs }: { flows: Flow[]; runs: RunSummary[] }) {
  const left = [...new Set(flows.map((f) => f.trigger.system))].sort();
  const right = [...new Set(flows.flatMap((f) => f.targets.filter((t) => t !== f.trigger.system)))].sort();
  const row = 38;
  const top = 50;
  const height = top + Math.max(left.length, right.length) * row + 8;
  const W = 820;
  const nodeW = 200;
  const ly = (name: string) => top + (left.indexOf(name) + 0.5) * row * (Math.max(left.length, right.length) / left.length);
  const ry = (name: string) => top + (right.indexOf(name) + 0.5) * row * (Math.max(left.length, right.length) / right.length);
  const hub = { x: W / 2, w: 150 };
  const edges = flows.flatMap((f) => {
    const mine = runs.filter((r) => r.workflow === f.name);
    const tone = mine.some((r) => r.status === "failed") ? "bad" : mine.some((r) => r.pending) ? "info" : mine.length > 0 ? "ok" : "idle";
    return f.targets
      .filter((t) => t !== f.trigger.system)
      .map((t) => ({ flow: f, from: f.trigger.system, to: t, tone, back: f.targets.includes(f.trigger.system) }));
  });
  return (
    <figure className="cmap" aria-label="Which systems send events to which, through Turgon">
      <div className="cmap-scroll">
      <svg viewBox={`0 0 ${W} ${height}`} role="img">
        <text x={nodeW / 2} y={16} className="cmap-head" textAnchor="middle">Events come from</text>
        <text x={W / 2} y={16} className="cmap-head" textAnchor="middle">Turgon</text>
        <text x={W - nodeW / 2} y={16} className="cmap-head" textAnchor="middle">Writes go to</text>
        <rect x={hub.x - hub.w / 2} y={top - 6} width={hub.w} height={height - top} rx={10} className="cmap-hub" />
        <text x={W / 2} y={32} className="cmap-hub-text" textAnchor="middle">map · match · dry run · approve · undo</text>
        {edges.map((e, i) => {
          const y1 = ly(e.from);
          const y2 = ry(e.to);
          const x1 = nodeW;
          const x2 = W - nodeW;
          const d = `M${x1},${y1} C${x1 + 150},${y1} ${x2 - 150},${y2} ${x2},${y2}`;
          return (
            <a key={i} href={`#flow-${e.flow.name}`} className={`cmap-edge tone-${e.tone}`}>
              <title>
                {e.flow.name}: {e.from} {e.flow.trigger.event} → {e.to}
                {e.back ? `, written back to ${e.from}` : ""}
              </title>
              <path d={d} />
            </a>
          );
        })}
        {left.map((n) => (
          <g key={"l" + n} className="cmap-node">
            <rect x={0} y={ly(n) - 14} width={nodeW} height={28} rx={7} className="cmap-src" />
            <text x={12} y={ly(n) + 4}>{n}</text>
          </g>
        ))}
        {right.map((n) => (
          <g key={"r" + n} className="cmap-node">
            <rect x={W - nodeW} y={ry(n) - 14} width={nodeW} height={28} rx={7} className="cmap-dst" />
            <text x={W - nodeW + 12} y={ry(n) + 4}>{n}</text>
          </g>
        ))}
      </svg>
      </div>
      <figcaption className="muted small">
        One line per flow; hover for its name, click to jump to it. Colors: <span className="key tone-info">waiting for approval</span>{" "}
        <span className="key tone-bad">has failed runs</span> <span className="key tone-ok">has run</span>{" "}
        <span className="key tone-idle">no runs yet</span>. Flows that also write back to their source say so on hover.
      </figcaption>
    </figure>
  );
}

// Sample data for the demo build (Vercel). It mirrors what the real API
// returns for the example recipes, and keeps decisions in memory only.
import type { AuditEntry, AuditLog, CatalogReport, Link, LinkResult, PendingApproval, RunDetail, RunSummary, StewardItem, User } from "./types";

// createDemoApi builds the sample state. Only the demo build calls it, so
// production bundles drop it entirely.
export function createDemoApi() {
  const now = Date.now();
  const at = (minutesAgo: number) => new Date(now - minutesAgo * 60_000).toISOString();

  const recipe = (name: string) => ({ id: `turgon/recipe/${name}`, roles: ["integration-operator"] });

  const erpOrder: PendingApproval = {
    step: "03-write",
    digest: "2c5dbdc3480711a3a9f0c1d2e3f40516273849a0b1c2d3e4f5061728394a5b6c",
    since: at(4),
    reasons: ["high-risk tool", "amount above approval threshold"],
    request: {
      target: "erp-db",
      operation: "create-sales-order",
      tool: "create_sales_order",
      risk: "high",
      subject: recipe("shop-orders-to-erp"),
      idempotencyKey: "SHOP-3002",
      entity: "SalesOrder",
      amount: 64000,
      simulate: true,
      compensation: "cancel-sales-order",
      payload: {
        externalId: "SHOP-3002", customerRef: "ada@example.com", customerId: "C-100", orderDate: "2026-09-24",
        netValue: 64000, currency: "EUR", lines: [{ material: "M-3", quantity: 40 }],
      },
    },
    preview: {
      mode: "rollback",
      row: {
        id: 8, external_id: "SHOP-3002", customer_id: "C-100", order_date: "2026-09-24", net_value: 64000.0,
        currency: "EUR", lines: [{ material: "M-3", quantity: 40 }], status: "open",
      },
    },
  };

  const sfLink: PendingApproval = {
    step: "05-write",
    digest: "9e1f7a3b5c7d9e1f2a4b6c8d0e2f4a6b8c0d2e4f6a8b0c2d4e6f8a0b2c4d6e8f",
    since: at(11),
    reasons: ["required by the recipe"],
    request: {
      target: "salesforce-prod",
      operation: "update-opportunity",
      tool: "update_opportunity",
      risk: "low",
      subject: recipe("salesforce-won-deals-to-erp"),
      idempotencyKey: "006000000000002AAA",
      entity: "Opportunity",
      simulate: true,
      compensation: "restore-opportunity",
      payload: { opportunityId: "006000000000002AAA", erpOrderNumber: "7" },
    },
    preview: {
      mode: "preview", sobject: "Opportunity", id: "006000000000002AAA",
      current: { ERP_Order_Number__c: null },
      proposed: { ERP_Order_Number__c: "7" },
    },
  };

  const agentOrder: PendingApproval = {
    step: "agent-write",
    digest: "5b8e2d4f6a1c3e5b7d9f0a2c4e6b8d0f1a3c5e7b9d1f3a5c7e9b1d3f5a7c9e1b",
    since: at(2),
    reasons: ["high-risk tool"],
    request: {
      target: "erp-db",
      operation: "create-sales-order",
      tool: "create_sales_order",
      risk: "high",
      subject: { id: "claude", roles: ["integration-operator"], agent: true, onBehalfOf: "sam@example.com" },
      idempotencyKey: "agent:claude:quote-4471",
      entity: "SalesOrder",
      amount: 2350,
      simulate: true,
      reason: "Customer accepted quote 4471 by email",
      payload: {
        currency: "EUR", customerId: "C-100", externalId: "QUOTE-4471", lines: [{ material: "M-2", quantity: 10 }],
        netValue: 2350, orderDate: "2026-09-24",
      },
    },
    preview: {
      mode: "rollback",
      row: {
        id: 9, external_id: "QUOTE-4471", customer_id: "C-100", order_date: "2026-09-24", net_value: 2350.0,
        currency: "EUR", lines: [{ material: "M-2", quantity: 10 }], status: "open",
      },
    },
  };

  const unresolved = (id: string, minutesAgo: number, link: StewardItem["runs"][number]["link"], payload: unknown): RunDetail => ({
    id, runId: `u-${id}`, workflow: id.slice(0, id.lastIndexOf("/")), status: "failed", started: at(minutesAgo), closed: at(minutesAgo - 0.1),
    failureType: "TurgonUnresolved", failure: `${link.entity} "${link.ref}" from ${link.system} has no master record; it needs a data steward`,
    event: { id: id.slice(id.lastIndexOf("/") + 1), position: 1, name: link.system === "stripe-billing" ? "Invoice.Paid" : "Order.Created", payload },
  });
  const grace = { entity: "Customer", system: "shopify-store", ref: "grace@lovelace-gmbh.example" };
  const cusNew = { entity: "Customer", system: "stripe-billing", ref: "cus_Q8newCustomer" };

  let runs: RunDetail[] = [
    unresolved("shopify-store-orders-to-erp/450789481", 7, grace, { id: 450789481, name: "#1012", email: "Grace@Lovelace-GmbH.example", subtotal_price: "129.00", currency: "EUR" }),
    unresolved("shopify-store-orders-to-erp/450789477", 19, grace, { id: 450789477, name: "#1008", email: "Grace@Lovelace-GmbH.example", subtotal_price: "54.50", currency: "EUR" }),
    unresolved("stripe-payments-to-erp/evt_0042", 33, cusNew, { id: "evt_0042", type: "invoice.paid", data: { object: { id: "in_0042", customer: "cus_Q8newCustomer", amount_paid: 49000, currency: "eur" } } }),

    { id: "create_sales_order/claude:quote-4471", runId: "r7", workflow: "create_sales_order", status: "running", started: at(2),
      pending: agentOrder, request: agentOrder.request },
    { id: "shop-orders-to-erp/6", runId: "r6", workflow: "shop-orders-to-erp", status: "running", started: at(4), pending: erpOrder,
      event: { id: "6", position: 6, name: "Order.Created", payload: { order_number: 3002, total: "64000.00", currency: "eur", customer: { email: "ada@example.com" } } } },
    { id: "salesforce-won-deals-to-erp/006000000000002AAA", runId: "r5", workflow: "salesforce-won-deals-to-erp", status: "running", started: at(11), pending: sfLink,
      event: { id: "006000000000002AAA", position: 1790261000000, name: "Opportunity.ClosedWon", payload: { Id: "006000000000002AAA", AccountId: "001000000000001AAA", Amount: 4200 } } },
    { id: "shop-orders-to-erp/5", runId: "r4", workflow: "shop-orders-to-erp", status: "completed", started: at(26), closed: at(25.6),
      result: { writes: [{ step: "03-write", endpoint: "erp-db", operation: "create-sales-order", status: "committed", result: { id: 7, external_id: "SHOP-3001", net_value: 1480.0, status: "open" } }] },
      event: { id: "5", position: 5, name: "Order.Created", payload: { order_number: 3001, total: "1480.00", currency: "eur" } } },
    { id: "salesforce-won-deals-to-erp/006000000000001AAA", runId: "r3", workflow: "salesforce-won-deals-to-erp", status: "completed", started: at(58), closed: at(57.8),
      result: { writes: [
        { step: "03-write", endpoint: "erp-db", operation: "create-sales-order", status: "committed", result: { id: 6, external_id: "006000000000001AAA", net_value: 7800.0 } },
        { step: "05-write", endpoint: "salesforce-prod", operation: "update-opportunity", status: "committed", result: { sobject: "Opportunity", id: "006000000000001AAA", previous: { ERP_Order_Number__c: null }, applied: { ERP_Order_Number__c: "6" } } },
      ] },
      event: { id: "006000000000001AAA", position: 1790259000000, name: "Opportunity.ClosedWon", payload: { Id: "006000000000001AAA", Amount: 7800 } } },
    { id: "shop-orders-to-erp/2", runId: "r2", workflow: "shop-orders-to-erp", status: "failed", started: at(95), closed: at(94.9),
      failure: "writeguard: simulation: write payload invalid: null value in column \"lines\" violates not-null constraint", failureType: "TurgonInvalid",
      result: undefined, event: { id: "2", position: 2, name: "Order.Created", payload: { order_number: 2002, total: "99000", items: [] } } },
  ];

  const entries: AuditEntry[] = [];
  function audit(actor: string, action: string, data: Record<string, unknown>, minutesAgo: number) {
    const seq = entries.length + 1;
    entries.push({ seq, time: at(minutesAgo), actor, action, data, prev: seq === 1 ? "0".repeat(64) : `demo${seq - 1}`, hash: `demo${seq}` });
  }
  audit("turgon/recipe/salesforce-won-deals-to-erp", "writeback.simulated", { target: "erp-db", operation: "create-sales-order" }, 58);
  audit("controller@example.com", "writeback.approval", { target: "erp-db", operation: "create-sales-order", status: "approved", note: "Matches PO 4471", key: "006000000000001AAA" }, 57.9);
  audit("turgon/recipe/salesforce-won-deals-to-erp", "writeback.committed", { target: "erp-db", operation: "create-sales-order", key: "006000000000001AAA" }, 57.9);
  audit("turgon/recipe/salesforce-won-deals-to-erp", "writeback.committed", { target: "salesforce-prod", operation: "update-opportunity", key: "006000000000001AAA" }, 57.8);
  audit("turgon/recipe/shop-orders-to-erp", "writeback.simulated", { target: "erp-db", operation: "create-sales-order" }, 26);
  audit("controller@example.com", "writeback.approval", { target: "erp-db", operation: "create-sales-order", status: "approved", key: "SHOP-3001" }, 25.7);
  audit("turgon/recipe/shop-orders-to-erp", "writeback.committed", { target: "erp-db", operation: "create-sales-order", key: "SHOP-3001" }, 25.6);

  const catalog: CatalogReport = {
    reports: [
      { subject: "Recipe/salesforce-won-deals-to-erp@1.0.0", level: "L1", deployable: true },
      { subject: "Recipe/salesforce-won-deals-to-sap-orders@1.0.0", level: "L0", deployable: true },
      { subject: "Recipe/shop-orders-to-erp@1.0.0", level: "L1", deployable: true },
      {
        subject: "Recipe/shopify-orders-to-sap@1.0.0", level: "L1", deployable: false,
        findings: [
          { stage: "resolve", severity: "error", message: "\"commerce\" is a stack slot, not a connector; verify this recipe within a StackBlueprint that fills the slot" },
        ],
        reviewQueue: [
          { mapping: "shopify-order-to-order@1.1.0", target: "incoterms", expression: "shipping_lines[0].code = 'pickup' ? 'EXW' : 'DAP'", origin: "ai", confidence: 0.81,
            rationale: "Pickup orders leave from our warehouse (EXW); everything else is delivered (DAP)." },
        ],
      },
      {
        subject: "StackBlueprint/eu-distributor-core", level: "L1", deployable: true,
        findings: [{ stage: "contract", severity: "warning", message: "recipe salesforce-won-deals-to-sap-orders names tool \"sap-ecc\" directly; naming slot \"erp\" would survive a swap" }],
        children: [
          { subject: "Plugin/sap-ecc@0.4.0", level: "L0", deployable: true },
          { subject: "Plugin/salesforce@3.1.0", level: "L0", deployable: true },
          { subject: "Plugin/credit-check@1.2.0", level: "L0", deployable: true },
        ],
      },
    ],
  };

  const user: User = { id: "you@example.com (demo)", roles: ["viewer", "approver", "steward", "operator"] };
  // Approvals a person rejected, so a retry can ask again.
  const rejected = new Map<string, NonNullable<RunDetail["pending"]>>();
  const delay = <T,>(v: T) => new Promise<T>((r) => setTimeout(() => r(structuredClone(v)), 120));
  const summary = ({ id, runId, workflow, status, started, closed, pending }: RunDetail): RunSummary => ({ id, runId, workflow, status, started, closed, pending });

  const linkOf = (r: RunDetail): Link | null => {
    const m = r.failureType === "TurgonUnresolved" ? /^(\S+) "(.+)" from (\S+) has no master record/.exec(r.failure ?? "") : null;
    return m ? { entity: m[1] ?? "", ref: m[2] ?? "", system: m[3] ?? "" } : null;
  };
  // What the matcher would suggest: the demo store's company domain is known.
  const suggestionsFor = (l: Link) =>
    l.ref.endsWith("@lovelace-gmbh.example") ? [{ master: "C-100", score: 0.877, reasons: ["same company email domain"] }] : [];

  function stewardItems(): StewardItem[] {
    const items = new Map<string, StewardItem>();
    for (const r of runs) {
      const link = linkOf(r);
      if (!link || r.status !== "failed") continue;
      const key = `${link.entity}/${link.system}/${link.ref}`;
      const it: StewardItem = items.get(key) ?? { ...link, suggestions: suggestionsFor(link), since: r.closed ?? r.started, runs: [] };
      it.runs.push({ id: r.id, workflow: r.workflow, started: r.started, failed: r.closed ?? r.started, link, event: r.event });
      if ((r.closed ?? r.started) < it.since) it.since = r.closed ?? r.started;
      items.set(key, it);
    }
    return [...items.values()].sort((a, b) => a.since.localeCompare(b.since));
  }

  return {
    me: () => delay(user),
    steward: () => delay(stewardItems()),
    link: (l: { entity: string; system: string; ref: string; master: string; note?: string }): Promise<LinkResult> => {
      const waiting = runs.filter((r) => {
        const k = linkOf(r);
        return r.status === "failed" && k && k.entity === l.entity && k.system === l.system && k.ref === l.ref;
      });
      if (waiting.length === 0) return Promise.reject(new Error("no run is waiting on this record any more; reload"));
      audit(user.id, "xref.linked", { ...l, runs: waiting.map((r) => r.id) }, 0);
      audit(user.id, "run.retried", { runs: waiting.map((r) => r.id), reason: `linked ${l.ref} to ${l.master}` }, 0);
      runs = runs.map((r) =>
        waiting.includes(r)
          ? { ...r, status: "completed", closed: new Date().toISOString(), failure: undefined, failureType: undefined,
              result: { writes: [{ step: "03-write", endpoint: "erp-db", operation: r.workflow.startsWith("stripe") ? "record-payment" : "create-sales-order", status: "committed", result: { customer_id: l.master } }] } }
          : r,
      );
      return delay({ retried: waiting.map((r) => r.id) });
    },
    retry: (q: { id: string; note: string }) => {
      const r = runs.find((x) => x.id === q.id);
      if (!r) return Promise.reject(new Error("run not found"));
      if (!["failed", "timed_out", "terminated", "canceled"].includes(r.status)) {
        return Promise.reject(new Error(`only failed runs can be retried; this one is ${r.status}`));
      }
      audit(user.id, "run.retried", { runs: [r.id], reason: q.note, status: r.status, failure: r.failureType }, 0);
      const again = rejected.get(r.id);
      rejected.delete(r.id);
      const now = new Date().toISOString();
      // A rejected write is asked again; other failures recur until their cause is fixed.
      const next: RunDetail = again
        ? { ...r, status: "running", started: now, closed: undefined, failure: undefined, failureType: undefined, pending: { ...again, since: now } }
        : { ...r, started: now, closed: now };
      runs = runs.map((x) => (x.id === r.id ? next : x));
      return delay({ retried: r.id });
    },
    runs: () => delay(runs.map(summary)),
    run: (id: string) => {
      const r = runs.find((x) => x.id === id);
      return r ? delay(r) : Promise.reject(new Error("run not found"));
    },
    audit: (): Promise<AuditLog[]> =>
      delay([{ file: "Postgres audit log (demo)", ok: true, count: entries.length, head: `demo${entries.length}`, entries: [...entries].reverse() }]),
    catalog: () => delay(catalog),
    decide: (d: { runId: string; step: string; digest: string; decision: "approve" | "reject"; note?: string }) => {
      const r = runs.find((x) => x.id === d.runId);
      if (!r?.pending || r.pending.digest !== d.digest) return Promise.reject(new Error("this run is no longer waiting for that decision; reload"));
      const p = r.pending;
      const approved = d.decision === "approve";
      audit(user.id, "writeback.approval", { target: p.request.target, operation: p.request.operation, status: approved ? "approved" : "rejected", note: d.note, key: p.request.idempotencyKey }, 0);
      const done: RunDetail = { ...r, pending: undefined, closed: new Date().toISOString() };
      if (approved) {
        const who = p.request.subject;
        const actor = who.agent && who.onBehalfOf ? `${who.id} for ${who.onBehalfOf}` : who.id;
        audit(actor, "writeback.committed", { target: p.request.target, operation: p.request.operation, key: p.request.idempotencyKey }, 0);
        done.status = "completed";
        done.result = { writes: [{ step: p.step, endpoint: p.request.target, operation: p.request.operation, status: "committed", result: p.preview }] };
      } else {
        done.status = "failed";
        done.failureType = "TurgonRejected";
        done.failure = `write rejected by ${user.id}${d.note ? `: ${d.note}` : ""}`;
        rejected.set(r.id, p);
      }
      runs = runs.map((x) => (x.id === r.id ? done : x));
      return delay({ status: approved ? "approved" : "rejected" });
    },
  };
}

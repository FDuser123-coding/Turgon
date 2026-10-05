import { describe, expect, it } from "vitest";
import { ago, breaks, duration, flags, toPreview, values, where } from "./format";

describe("toPreview", () => {
  it("lists the row of a rolled-back insert", () => {
    const p = toPreview({ mode: "rollback", row: { net_value: 349.9, customer_id: "C-100" } });
    expect(p?.kind).toBe("rollback");
    expect(p?.rows.map((r) => r.field)).toEqual(["customer_id", "net_value"]);
  });

  it("marks which fields an update changes", () => {
    const p = toPreview({
      mode: "preview",
      sobject: "Opportunity",
      id: "006",
      current: { ERP_Order_Number__c: null, Stage: "Won" },
      proposed: { ERP_Order_Number__c: "6", Stage: "Won" },
    });
    expect(p?.kind).toBe("diff");
    expect(p?.rows).toEqual([
      { field: "ERP_Order_Number__c", current: null, proposed: "6", changed: true },
      { field: "Stage", current: "Won", proposed: "Won", changed: false },
    ]);
  });

  it("falls back to raw JSON and handles no preview", () => {
    expect(toPreview({ something: 1 })?.kind).toBe("raw");
    expect(toPreview(undefined)).toBeNull();
  });
});

describe("time", () => {
  const now = new Date("2026-09-24T12:00:00Z");
  it("formats relative times", () => {
    expect(ago("2026-09-24T11:59:58Z", now)).toBe("just now");
    expect(ago("2026-09-24T11:45:00Z", now)).toBe("15 min ago");
    expect(ago("2026-09-23T00:00:00Z", now)).toBe("36 h ago");
    expect(ago("2026-09-22T12:00:00Z", now)).toBe("2 days ago");
  });
  it("formats durations", () => {
    expect(duration("2026-09-24T12:00:00Z", "2026-09-24T12:00:02.500Z")).toBe("2.5 s");
    expect(duration("2026-09-24T12:00:00Z", "2026-09-24T13:05:00Z")).toBe("1 h 5 min");
  });
});

describe("metadata changes", () => {
  const removed = { kind: "field-removed", object: "erp.sales_orders", field: "credit_status", old: "text", breaking: true, usedBy: ["operation get-sales-order"] };
  it("breaks only what something uses", () => {
    expect(breaks(removed)).toBe(true);
    expect(breaks({ ...removed, usedBy: [] })).toBe(false);
    expect(breaks({ ...removed, breaking: false })).toBe(false);
  });
  it("names the field and its values", () => {
    expect(where(removed)).toBe("erp.sales_orders.credit_status");
    expect(where({ object: "Opportunity" })).toBe("Opportunity");
    expect(values(removed)).toBe("was text");
    expect(values({ ...removed, kind: "length-shrunk", old: "3", new: "2" })).toBe("3 → 2");
    expect(values({ ...removed, kind: "field-added", old: undefined, new: "text" })).toBe("text");
  });
  it("lists a field's flags", () => {
    expect(flags({ name: "id", type: "bigint", key: true, required: true })).toEqual(["key", "required"]);
    expect(flags({ name: "total", type: "bigint", readOnly: true })).toEqual(["read-only"]);
  });
});

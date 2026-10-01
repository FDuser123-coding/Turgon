import http.client
import json
import os
import sys
import threading
import unittest
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import turgon_splink as ts  # noqa: E402

PEOPLE = [
    # system, id, master, email, name
    ("crm", "c1", "M1", "ada@acme.io", "ada lovelace"),
    ("erp", "e1", "M1", "ada.l@acme.io", "ada lovelace"),
    ("crm", "c2", "M2", "alan@turing.org", "alan turing"),
    ("erp", "e2", "M2", "alan@turing.org", "alan m turing"),
    ("crm", "c3", "M3", "grace@navy.mil", "grace hopper"),
    ("erp", "e3", "M3", "ghopper@navy.mil", "grace hopper"),
    ("crm", "c4", "M4", "linus@kernel.org", "linus torvalds"),
    ("crm", "c5", "M5", "ken@bell-labs.com", "ken thompson"),
    ("erp", "e5", "M5", "ken@bell-labs.com", "kenneth thompson"),
    ("crm", "c6", "M6", "dennis@bell-labs.com", "dennis ritchie"),
]


def links():
    out = []
    for system, sid, master, email, name in PEOPLE:
        attrs = {"email": email, "name": name, "domain": email.split("@")[1]}
        out.append(ts.Link(system, sid, master, attrs))
    return out


class ColumnTest(unittest.TestCase):
    def test_column_for(self):
        self.assertEqual(ts.column_for("email"), "email")
        self.assertEqual(ts.column_for("name"), "name")
        self.assertEqual(ts.column_for("exact:vatId"), "exact_vatid")
        self.assertEqual(ts.column_for("exact:tax-code; drop"), "exact_tax_code__drop")
        self.assertIsNone(ts.column_for("phone"))


class MatchTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.model = ts.train("Customer", links(), "d1")

    def test_trained(self):
        m = self.model
        self.assertEqual(m.records, 10)
        self.assertEqual(m.master_count, 6)
        self.assertEqual(m.trained, ["m", "u"])
        self.assertEqual(m.columns, ["domain", "email", "name"])
        s = m.summary()
        self.assertEqual(s["records"], 10)
        self.assertNotIn("ada", json.dumps(s))  # no record data

    def test_same_email_and_name(self):
        got = ts.match(self.model, {"email": "alan@turing.org", "name": "alan turing", "domain": "turing.org"})
        self.assertEqual(got[0]["master"], "M2")
        self.assertGreater(got[0]["probability"], 0.95)
        self.assertIn("same email address", got[0]["reasons"])
        self.assertIn("same name", got[0]["reasons"])

    def test_domain_counts_only_when_emails_differ(self):
        got = ts.match(self.model, {"email": "a.lovelace@acme.io", "name": "ada lovelace", "domain": "acme.io"})
        self.assertEqual(got[0]["master"], "M1")
        self.assertIn("same company email domain", got[0]["reasons"])
        same = ts.match(self.model, {"email": "ada@acme.io", "domain": "acme.io"})
        self.assertEqual(same[0]["reasons"], ["same email address"])

    def test_shared_domain_is_ambiguous(self):
        got = ts.match(self.model, {"email": "someone@bell-labs.com", "domain": "bell-labs.com"}, limit=5)
        masters = {s["master"] for s in got}
        self.assertEqual(masters, {"M5", "M6"})
        self.assertLess(got[0]["probability"], 0.95)

    def test_one_suggestion_per_master_best_first(self):
        got = ts.match(self.model, {"name": "grace hopper"}, limit=5)
        self.assertEqual([s["master"] for s in got][:1], ["M3"])
        self.assertEqual(len({s["master"] for s in got}), len(got))
        probs = [s["probability"] for s in got]
        self.assertEqual(probs, sorted(probs, reverse=True))

    def test_similar_name(self):
        got = ts.match(self.model, {"name": "linus torvald"})
        self.assertEqual(got[0]["master"], "M4")
        self.assertTrue(set(got[0]["reasons"]) & {"same name", "similar name"})

    def test_nothing_in_common(self):
        self.assertEqual(ts.match(self.model, {"email": "nobody@nowhere.example"}), [])
        self.assertEqual(ts.match(self.model, {"phone": "123"}), [])

    def test_limit(self):
        got = ts.match(self.model, {"email": "x@bell-labs.com", "domain": "bell-labs.com"}, limit=1)
        self.assertEqual(len(got), 1)


class SmoothTest(unittest.TestCase):
    def test_smooth(self):
        start = {"comparisons": [{"comparison_levels": [
            {"is_null_level": True},
            {"m_probability": 0.9, "u_probability": 0.01},
            {"m_probability": 0.1, "u_probability": 0.99},
        ]}]}
        trained = {"comparisons": [{"comparison_levels": [
            {"is_null_level": True},
            {"m_probability": 0.5, "u_probability": None},
            {"m_probability": 0.5, "u_probability": 0.0},
        ]}]}
        got = ts.smooth(start, trained, 20, 0)["comparisons"][0]["comparison_levels"]
        self.assertAlmostEqual(got[1]["m_probability"], 0.7)  # (20*0.5 + 20*0.9) / 40
        self.assertEqual(got[1]["u_probability"], 0.01)  # not observed: the start
        self.assertEqual(got[2]["u_probability"], 0.99)  # no pairs: the start
        got = ts.smooth(start, trained, 20, 1_000_000)["comparisons"][0]["comparison_levels"]
        self.assertGreater(got[2]["u_probability"], 0)  # clamped away from 0

    def test_few_links_do_not_swing(self):
        m = ts.train("Customer", links())
        name = m.linker.misc.save_model_to_json()["comparisons"][1]["comparison_levels"]
        self.assertLess(name[1]["m_probability"], 0.95)


class ExactTest(unittest.TestCase):
    def test_exact_field(self):
        ls = [
            ts.Link("crm", "1", "A", {"exact:vatId": "IT123", "name": "acme spa"}),
            ts.Link("erp", "1", "A", {"exact:vatId": "IT123", "name": "acme s p a"}),
            ts.Link("crm", "2", "B", {"exact:vatId": "IT999", "name": "beta srl"}),
        ]
        m = ts.train("Company", ls)
        got = ts.match(m, {"exact:vatId": "IT123"})
        self.assertEqual(got[0]["master"], "A")
        self.assertEqual(got[0]["reasons"], ["same vatId"])

    def test_single_record(self):
        m = ts.train("Company", [ts.Link("crm", "1", "A", {"name": "acme"})])
        self.assertEqual(m.trained, [])
        self.assertEqual(ts.match(m, {"name": "acme"})[0]["master"], "A")

    def test_no_links(self):
        self.assertIsNone(ts.train("Company", []))
        self.assertIsNone(ts.train("Company", [ts.Link("crm", "1", "A", {"phone": "1"})]))


class ModelsTest(unittest.TestCase):
    def test_retrains_on_change(self):
        data = {"Customer": ("d1", links())}
        models = ts.Models(lambda: data)
        self.assertFalse(models.ready)
        self.assertEqual(models.refresh(), ["Customer"])
        self.assertTrue(models.ready)
        self.assertEqual(models.refresh(), [])
        self.assertEqual(models.refresh(force=True), ["Customer"])
        data["Customer"] = ("d2", links()[:4])
        self.assertEqual(models.refresh(), ["Customer"])
        self.assertEqual(models.get("Customer").records, 4)
        data.clear()
        models.refresh()
        self.assertIsNone(models.get("Customer"))
        self.assertEqual(models.summary(), {})


class TrainFailureTest(unittest.TestCase):
    def test_one_entity_failing_keeps_its_model_and_trains_the_others(self):
        data = {"Customer": ("d1", links()), "Supplier": ("s1", [ts.Link("erp", "1", "S", {"name": "acme"})])}
        models = ts.Models(lambda: data)
        models.refresh()
        real = ts.train

        def failing(entity, *a, **k):
            if entity == "Customer":
                raise RuntimeError("boom")
            return real(entity, *a, **k)

        data["Customer"] = ("d2", links()[:3])
        data["Supplier"] = ("s2", [ts.Link("erp", "1", "S", {"name": "acme"}), ts.Link("crm", "1", "S", {"name": "acme"})])
        ts.train = failing
        try:
            self.assertEqual(models.refresh(), ["Supplier"])
        finally:
            ts.train = real
        self.assertEqual(models.get("Customer").records, 10)
        self.assertEqual(models.get("Supplier").records, 2)
        self.assertEqual(models.refresh(), ["Customer"])  # retried on the next refresh


class HTTPTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.models = ts.Models(lambda: {"Customer": ("d1", links())})
        cls.srv = ThreadingHTTPServer(("127.0.0.1", 0), ts.handler(cls.models, "s3cret"))
        threading.Thread(target=cls.srv.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()

    def call(self, method, path, body=None, token="s3cret"):
        c = http.client.HTTPConnection("127.0.0.1", self.srv.server_address[1], timeout=30)
        h = {"Content-Type": "application/json"}
        if token:
            h["Authorization"] = "Bearer " + token
        c.request(method, path, body=None if body is None else (body if isinstance(body, bytes) else json.dumps(body)), headers=h)
        r = c.getresponse()
        out = (r.status, json.loads(r.read() or b"{}"))
        c.close()
        return out

    def test_flow(self):
        # Not trained yet.
        self.assertEqual(self.call("GET", "/healthz")[0], 503)
        st, _ = self.call("POST", "/v1/match", {"entity": "Customer", "attributes": {"name": "x"}})
        self.assertEqual(st, 503)
        st, body = self.call("POST", "/v1/train")
        self.assertEqual(st, 200)
        self.assertEqual(body["retrained"], ["Customer"])

        st, body = self.call("GET", "/healthz", token="")
        self.assertEqual(st, 200)
        self.assertEqual(body["entities"]["Customer"]["masters"], 6)

        st, body = self.call("POST", "/v1/match", {"entity": "Customer", "attributes": {"email": "alan@turing.org", "name": "alan turing"}, "limit": 2})
        self.assertEqual(st, 200)
        self.assertEqual(body["candidates"][0]["master"], "M2")
        self.assertLessEqual(len(body["candidates"]), 2)
        self.assertEqual(body["model"]["records"], 10)

        st, body = self.call("POST", "/v1/match", {"entity": "Supplier", "attributes": {"name": "x"}})
        self.assertEqual(st, 404)
        self.assertIn("no links", body["error"])

        self.assertEqual(self.call("POST", "/v1/match", {"entity": "Customer"})[0], 400)
        self.assertEqual(self.call("POST", "/v1/match", b"not json")[0], 400)
        self.assertEqual(self.call("POST", "/v1/match", ["list"])[0], 400)
        self.assertEqual(self.call("POST", "/v1/nope", {})[0], 404)
        self.assertEqual(self.call("GET", "/nope")[0], 404)

        self.assertEqual(self.call("POST", "/v1/match", {"entity": "Customer", "attributes": {}}, token="")[0], 401)
        self.assertEqual(self.call("POST", "/v1/match", {"entity": "Customer", "attributes": {}}, token="wrong")[0], 401)
        self.assertEqual(self.call("POST", "/v1/train", token="")[0], 401)


if __name__ == "__main__":
    unittest.main()


@unittest.skipUnless(os.environ.get("TURGON_TEST_DATABASE_URL"), "TURGON_TEST_DATABASE_URL is not set")
class PostgresLoaderTest(unittest.TestCase):
    """The loader against turgon_xref as Turgon creates it (pkg/store/pgstore)."""

    def setUp(self):
        import psycopg

        self.url = os.environ["TURGON_TEST_DATABASE_URL"]
        self.schema = f"splink_test_{os.getpid()}"
        self.conn = psycopg.connect(self.url, autocommit=True)
        self.conn.execute(f"CREATE SCHEMA {self.schema}")
        self.conn.execute(f"""CREATE TABLE {self.schema}.turgon_xref (
            entity text NOT NULL, system text NOT NULL, source_id text NOT NULL, master_id text NOT NULL,
            attributes jsonb NOT NULL DEFAULT '{{}}', PRIMARY KEY (entity, system, source_id))""")
        sep = "&" if "?" in self.url else "?"
        self.scoped = f"{self.url}{sep}options=-csearch_path%3D{self.schema}"

    def tearDown(self):
        self.conn.execute(f"DROP SCHEMA {self.schema} CASCADE")
        self.conn.close()

    def insert(self, entity, system, sid, master, attrs):
        self.conn.execute(
            f"INSERT INTO {self.schema}.turgon_xref VALUES (%s, %s, %s, %s, %s) "
            "ON CONFLICT (entity, system, source_id) DO UPDATE SET master_id = EXCLUDED.master_id, attributes = EXCLUDED.attributes",
            (entity, system, sid, master, json.dumps(attrs)))

    def test_load_and_retrain(self):
        for system, sid, master, email, name in PEOPLE:
            self.insert("Customer", system, sid, master, {"email": email, "name": name})
        self.insert("Supplier", "erp", "s1", "S1", {"exact:vatId": "IT1"})
        load = ts.postgres_loader(self.scoped)
        got = load()
        self.assertEqual(sorted(got), ["Customer", "Supplier"])
        digest, links = got["Customer"]
        self.assertEqual(len(links), 10)
        self.assertEqual({l.master for l in links}, {"M1", "M2", "M3", "M4", "M5", "M6"})
        self.assertIn("email", links[0].attributes)

        models = ts.Models(load)
        self.assertEqual(sorted(models.refresh()), ["Customer", "Supplier"])
        self.assertEqual(models.refresh(), [])
        self.insert("Customer", "shop", "x1", "M4", {"email": "linus@kernel.org"})
        self.assertEqual(models.refresh(), ["Customer"])
        self.assertEqual(models.get("Customer").records, 11)
        self.assertEqual(load()["Supplier"][0], got["Supplier"][0])

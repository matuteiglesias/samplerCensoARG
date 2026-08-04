import csv
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from censo_sampler.release import ReleaseError, build_release, check_release


ROOT = Path(__file__).parents[1]
FIXTURE = ROOT / "fixtures" / "cpv2010_valid"


class ReleaseTests(unittest.TestCase):
    def build(self, root, source=FIXTURE, seed=20260804, fraction=0.5):
        return build_release(source, root, fraction=fraction, seed=seed, analysis_period="2024-Q1",
                             geography_path=source / "GEOGRAPHY.csv", max_households=20)

    def ids(self, release, table, key):
        with (release / table).open() as stream:
            return {row[key] for row in csv.DictReader(stream)}

    def test_same_seed_is_stable_and_order_independent(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            first = self.build(tmp / "one")
            shuffled = tmp / "source"
            shutil.copytree(FIXTURE, shuffled)
            for filename in ("HOGAR.csv", "PERSONA.csv"):
                lines = (shuffled / filename).read_text().splitlines()
                (shuffled / filename).write_text("\n".join([lines[0], *reversed(lines[1:])]) + "\n")
            second = self.build(tmp / "two", shuffled)
            self.assertEqual(self.ids(first, "households.csv", "sample_household_id"),
                             self.ids(second, "households.csv", "sample_household_id"))

    def test_different_seed_changes_selection_and_relations_are_complete(self):
        with tempfile.TemporaryDirectory() as tmp:
            first = self.build(Path(tmp) / "one", seed=1)
            second = self.build(Path(tmp) / "two", seed=2)
            first_ids = self.ids(first, "households.csv", "sample_household_id")
            self.assertNotEqual(first_ids, self.ids(second, "households.csv", "sample_household_id"))
            with (first / "persons.csv").open() as stream:
                self.assertTrue({r["sample_household_id"] for r in csv.DictReader(stream)} <= first_ids)
            check_release(first)

    def test_leading_zero_geography_certainty_and_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            release = self.build(Path(tmp), fraction=0.1)
            with (release / "persons.csv").open() as stream:
                self.assertTrue(any(row["radio_2010_id"].startswith("0") for row in csv.DictReader(stream)))
            manifest = json.loads((release / "manifest.json").read_text())
            self.assertIn("90084", manifest["qa"]["certainty_departments"])
            self.assertEqual("research.census-sample/v1", manifest["contract"])

    def test_duplicate_and_orphan_fixture_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = ROOT / "fixtures" / "cpv2010_invalid"
            with self.assertRaisesRegex(ReleaseError, "duplicate"):
                self.build(Path(tmp), source)


if __name__ == "__main__":
    unittest.main()

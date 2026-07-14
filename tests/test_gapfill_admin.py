# SPDX-FileCopyrightText: OpenAI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from __future__ import annotations

import unittest

from crawler_admin.gapfill_service import (
    build_gapfill_holdout_catalog,
    build_gapfill_holdout_view,
    build_gapfill_selftest_catalog,
    build_gapfill_selftest_view,
)


class GapfillAdminServiceTest(unittest.TestCase):
    def test_catalog_contains_display_metadata(self) -> None:
        catalog = {item.name: item for item in build_gapfill_selftest_catalog()}

        self.assertIn("missing_timestamp_gap", catalog)
        self.assertEqual(catalog["missing_timestamp_gap"].fault_type, "timestamp_gap")
        self.assertEqual(catalog["missing_timestamp_gap"].method, "linear")
        self.assertEqual(catalog["missing_timestamp_gap"].expected_filled, 2)
        self.assertGreater(catalog["missing_timestamp_gap"].source_rows, 0)

    def test_selftest_view_contains_results_and_chart_markers(self) -> None:
        view = build_gapfill_selftest_view(["donor_refined_seasonal_gap"])

        self.assertTrue(view.all_passed)
        self.assertEqual(view.passed_count, 1)
        self.assertEqual(view.failed_count, 0)
        self.assertEqual(view.selected_names, ["donor_refined_seasonal_gap"])
        self.assertEqual(len(view.results), 1)
        self.assertEqual(len(view.charts), 1)
        self.assertTrue(view.charts[0].has_data)
        self.assertEqual(view.charts[0].filled_count, 3)
        self.assertEqual(len(view.charts[0].filled_markers), 3)
        self.assertGreater(view.series_rows, 0)

    def test_holdout_catalog_contains_selectable_data_defaults(self) -> None:
        catalog = {item.name: item for item in build_gapfill_holdout_catalog()}

        self.assertIn("linear_hourly", catalog)
        self.assertEqual(catalog["linear_hourly"].method, "linear")
        self.assertGreater(catalog["linear_hourly"].recommended_gap_length, 0)
        self.assertGreaterEqual(catalog["linear_hourly"].max_gap_length, catalog["linear_hourly"].recommended_gap_length)

    def test_holdout_view_contains_error_metrics_and_truth_chart(self) -> None:
        view = build_gapfill_holdout_view(
            "linear_hourly",
            6,
            gap_start_index=24,
            fault_type="value_gap",
            method="linear",
        )

        self.assertTrue(view.passed)
        self.assertEqual(view.result.compared_points, 6)
        self.assertEqual(view.result.mean_absolute_error, 0.0)
        self.assertEqual(len(view.charts), 1)
        self.assertTrue(view.charts[0].truth_segments)
        self.assertEqual(view.charts[0].filled_count, 6)
        self.assertIn("MAE", {metric.label for metric in view.error_metrics})


if __name__ == "__main__":
    unittest.main()

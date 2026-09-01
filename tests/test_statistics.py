"""Tests for the pure-stdlib rank statistics in validate_ori.

These exist because a real bug shipped here: one series was filtered for
NULLs and the other was not, and zip() inside the correlation silently
truncated to the shorter list, correlating mismatched rows. A stored
correlation of +0.07 should have been -0.30.
"""

from __future__ import annotations

import math

import pytest

from validate_ori import (
    average_ranks,
    median,
    paired_values,
    pearson,
    spearman,
)


class TestAverageRanks:
    def test_ranks_are_one_based_and_ordered(self):
        assert average_ranks([10.0, 20.0, 30.0]) == [1.0, 2.0, 3.0]

    def test_ranks_follow_values_not_positions(self):
        assert average_ranks([30.0, 10.0, 20.0]) == [3.0, 1.0, 2.0]

    def test_tied_values_share_the_average_rank(self):
        # Positions 2 and 3 are tied, so both take (2 + 3) / 2 = 2.5.
        assert average_ranks([1.0, 5.0, 5.0, 9.0]) == [1.0, 2.5, 2.5, 4.0]

    def test_all_values_tied_gives_the_same_rank(self):
        assert average_ranks([7.0, 7.0, 7.0]) == [2.0, 2.0, 2.0]

    def test_empty_input(self):
        assert average_ranks([]) == []


class TestPearson:
    def test_perfect_positive_correlation(self):
        assert pearson([1.0, 2.0, 3.0], [2.0, 4.0, 6.0]) == pytest.approx(1.0)

    def test_perfect_negative_correlation(self):
        assert pearson([1.0, 2.0, 3.0], [6.0, 4.0, 2.0]) == pytest.approx(-1.0)

    def test_zero_variance_returns_zero_rather_than_dividing_by_zero(self):
        assert pearson([5.0, 5.0, 5.0], [1.0, 2.0, 3.0]) == 0.0

    def test_known_value(self):
        # Textbook example, verified by hand.
        xs = [1.0, 2.0, 3.0, 4.0, 5.0]
        ys = [2.0, 4.0, 5.0, 4.0, 5.0]
        assert pearson(xs, ys) == pytest.approx(0.7745966, abs=1e-6)


class TestSpearman:
    def test_monotonic_but_nonlinear_is_a_perfect_rank_correlation(self):
        # Pearson would be < 1 here; Spearman sees only the ordering.
        xs = [1.0, 2.0, 3.0, 4.0]
        ys = [1.0, 4.0, 9.0, 16.0]
        assert spearman(xs, ys) == pytest.approx(1.0)

    def test_reversed_order_is_minus_one(self):
        assert spearman([1.0, 2.0, 3.0, 4.0], [4.0, 3.0, 2.0, 1.0]) == pytest.approx(-1.0)

    def test_mismatched_lengths_raise_instead_of_truncating(self):
        # The original bug: zip() would have quietly used the first two
        # x values against a different pair of y values.
        with pytest.raises(ValueError, match="equal-length"):
            spearman([1.0, 2.0, 3.0], [1.0, 2.0])

    def test_too_few_points_returns_zero(self):
        assert spearman([1.0], [2.0]) == 0.0
        assert spearman([], []) == 0.0

    def test_handles_ties_without_error(self):
        result = spearman([1.0, 1.0, 2.0, 3.0], [5.0, 5.0, 2.0, 1.0])
        assert -1.0 <= result <= 1.0
        assert result < 0


class TestPairedValues:
    def test_keeps_only_rows_where_both_values_exist(self):
        records = [
            {"score": 10.0, "rate": 1.0},
            {"score": 20.0, "rate": None},
            {"score": 30.0, "rate": 3.0},
        ]
        xs, ys = paired_values(records, "score", "rate")
        assert xs == [10.0, 30.0]
        assert ys == [1.0, 3.0]

    def test_drops_the_row_when_either_side_is_missing(self):
        records = [
            {"score": None, "rate": 1.0},
            {"score": 20.0, "rate": 2.0},
            {"rate": 3.0},
        ]
        xs, ys = paired_values(records, "score", "rate")
        assert xs == [20.0]
        assert ys == [2.0]

    def test_output_lengths_always_match(self):
        records = [
            {"a": i if i % 2 else None, "b": None if i % 3 == 0 else i}
            for i in range(20)
        ]
        xs, ys = paired_values(records, "a", "b")
        assert len(xs) == len(ys)

    def test_alignment_is_preserved_so_the_correlation_is_correct(self):
        # Perfect negative relationship, with a hole in the middle. If the
        # hole misaligned the series, the correlation would not be -1.
        records = [
            {"score": 1.0, "rate": 5.0},
            {"score": 2.0, "rate": None},
            {"score": 3.0, "rate": 3.0},
            {"score": 4.0, "rate": 2.0},
            {"score": 5.0, "rate": 1.0},
        ]
        xs, ys = paired_values(records, "score", "rate")
        assert spearman(xs, ys) == pytest.approx(-1.0)


class TestMedian:
    def test_odd_length_returns_the_middle_value(self):
        assert median([3.0, 1.0, 2.0]) == 2.0

    def test_even_length_averages_the_two_middle_values(self):
        assert median([1.0, 2.0, 3.0, 4.0]) == 2.5

    def test_single_value(self):
        assert median([42.0]) == 42.0

    def test_empty_returns_nan(self):
        assert math.isnan(median([]))

    def test_does_not_mutate_the_caller_list(self):
        values = [3.0, 1.0, 2.0]
        median(values)
        assert values == [3.0, 1.0, 2.0]

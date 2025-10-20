import csv
from collections import defaultdict, Counter
from datetime import datetime
from io import StringIO
from re import L
from typing import List, Dict, Callable
import math
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm
from enum import Enum


# Configure system encoding
import sys

sys.stdin.reconfigure(encoding="utf-8")
sys.stdout.reconfigure(encoding="utf-8")

# Constants
DISTANCES = [1000, 1200, 1400, 1600, 1650, 1800, 2000, 2200]
RACE_CLASS_LEVELS = {
    "G1": 1,
    "G2": 1,
    "G3": 1,
    "1": 1,
    "2": 2,
    "4YO": 2,
    "3": 3,
    "3R": 3,
    "4": 4,
    "4R": 4,
    "5": 5,
    "GRIFFIN": 6,
}


# Data Classes
class Race:
    """Represents a single horse race with input data."""

    def __init__(self, data: dict):
        self.horse_id = data.get("horse_id")
        self.race_index = data.get("race_index")
        self.date = data.get("date")
        self.season = data.get("season")
        self.racecourse = data.get("racecourse")
        self.track = data.get("track")
        self.course = data.get("course")
        self.dist = data.get("dist")
        self.g = data.get("g")
        self.race_class = data.get("race_class")
        self.dr = data.get("dr")
        self.rtg = data.get("rtg")
        self.trainer = data.get("trainer")
        self.jockey = data.get("jockey")
        self.act_wt = data.get("act_wt")
        self.declar_horse_wt = data.get("declar_horse_wt")
        self.gear = data.get("gear")
        self.pla = data.get("pla")
        self.lbw = data.get("lbw")
        self.running_position = data.get("running_position")
        self.finish_time = data.get("finish_time")
        self.win_odds = data.get("win_odds")


class RaceWithStats(Race):
    """Extends Race with derived statistical variables."""

    def __init__(self, original: Race):
        super().__init__(vars(original))
        # Race performance metrics
        self.num_races_run: int = 0
        self.num_races_top1: int = 0
        self.num_races_top2: int = 0
        self.num_races_top3: int = 0

        self.percent_races_top1 = 0.0
        self.percent_races_top2 = 0.0
        self.percent_races_top3 = 0.0

        self.num_races_run_seasonal: int = 0

        # # day since
        self.days_since_last_run: int = 0
        self.days_since_last_run_log: float = 0.0
        self.days_since_last_run_sqrt: float = 0.0

        # # Jockey performance metrics
        self.jokey_num_races_run: int = 0
        # # normalization
        # self.normalized_dr: float = 0.0
        # self.normalized_act_wt: float = 0.0
        # self.normalized_act_wt_dist: float = 0.0
        # self.normalized_rtg: float = 0.0


class HorseRacingVariableCreator:
    """Computes derived variables for horse racing data using modular functions."""

    def __init__(self):
        self.variable_creators: List[Callable[[RaceWithStats, List[Race]], None]] = [
            self._compute_num_races_run,
            self._compute_num_races_run_seasonal,
            self._compute_days_since_last_run,
        ]
        self.jockey_variable_creators: List[Callable[[RaceWithStats, List[Race]], None]] = [
            self._compute_jockey_num_races_run,
        ]
        self.normalized_variable_creators: List[Callable[[List[RaceWithStats]], None]] = []

    @staticmethod
    def _normalize_pla_exponential(pla: int) -> float:
        decay_factor = 0.5
        return np.exp(-decay_factor * (pla - 1))

    # Race Performance Metrics
    def _compute_num_races_run(self, race_stats: RaceWithStats, prior_races: List[Race]) -> None:
        """Count total races run and top placements before the current race."""
        counts = Counter(race.pla for race in prior_races if race.pla in {"01", "02", "03"})

        num_races_run = len(prior_races)
        race_stats.num_races_run = num_races_run
        race_stats.num_races_top1 = counts["01"]
        race_stats.num_races_top2 = counts["01"] + counts["02"]
        race_stats.num_races_top3 = counts["01"] + counts["02"] + counts["03"]

        race_stats.percent_races_top1 = counts["01"] / num_races_run
        race_stats.percent_races_top2 = (counts["01"] + counts["02"]) / num_races_run
        race_stats.percent_races_top3 = (counts["01"] + counts["02"] + counts["03"]) / num_races_run

    def _compute_num_races_run_seasonal(self, race_stats: RaceWithStats, prior_races: List[Race]) -> None:
        """Count total races run and top placements before the current race."""
        race_stats.num_races_run_seasonal = sum(1 for race in prior_races if race.season == race_stats.season)

    def _compute_days_since_last_run(self, race_stats: RaceWithStats, prior_races: List[Race]) -> None:
        """Calculate days since the last race."""

        prev_date = prior_races[-1].date
        curr_date = race_stats.date
        days_since_last_run = (curr_date - prev_date).days

        race_stats.days_since_last_run = days_since_last_run
        race_stats.days_since_last_run_log = np.log1p(days_since_last_run)
        race_stats.days_since_last_run_sqrt = np.sqrt(days_since_last_run)

    # -------------------------------------------------------------------------------------------------------------------------------------
    # jockey level variable

    def _compute_jockey_num_races_run(self, race_stats: RaceWithStats, prior_races: List[Race]) -> None:
        """Count total races run and top placements before the current race."""
        race_stats.jokey_num_races_run = sum(1 for race in prior_races if race.season == race_stats.season)

    # -------------------------------------------------------------------------------------------------------------------------------------

    def compute_derived_variables(self, all_races: List[Race]) -> List[RaceWithStats]:
        """
        Compute derived variables for each race by grouping and processing horse and jockey data.

        Args:
            all_races: List of Race objects.
        Returns:
            List of RaceWithStats objects with derived variables.
        """
        all_races.sort(key=lambda r: (r.date, r.race_index))

        # Group races by horse_id
        horse_races_map: Dict[str, List[Race]] = defaultdict(list)
        jockey_races_map: Dict[str, List[Race]] = defaultdict(list)
        race_group_map: Dict[tuple, List[RaceWithStats]] = defaultdict(list)

        enhanced_races: List[RaceWithStats] = []

        for race in tqdm(all_races, desc="Calculating Races Variable"):
            race_stats = RaceWithStats(race)

            for func in self.variable_creators:
                prior_races = horse_races_map.get(race_stats.horse_id, [])
                if prior_races:
                    func(race_stats, prior_races)

            for func in self.jockey_variable_creators:
                prior_races = jockey_races_map.get(race_stats.jockey, [])
                if prior_races:
                    func(race_stats, prior_races)

            race_group_map[(race_stats.date, race_stats.race_index)].append(race_stats)
            horse_races_map[race.horse_id].append(race)
            jockey_races_map[race.jockey].append(race)

        # grouped races , normalized
        # for (date, race_index), race_stats_list in tqdm(race_group_map.items(), desc="Normalizing Races Variable"):
        #     for func in self.normalized_variable_creators:
        #         func(race_stats_list)

        return enhanced_races


def race_with_stats_to_csv(races: List[RaceWithStats]) -> str:
    """Convert a list of RaceWithStats objects to a CSV string and save to file."""
    output = StringIO()
    if not races:
        return ""
    fieldnames = vars(races[0]).keys()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for race in races:
        race_dict = vars(race)
        row = {key: str(value) for key, value in race_dict.items()}
        writer.writerow(row)
    csv_content = output.getvalue()
    with open("testing.csv", "w", newline="", encoding="utf-8") as csvfile:
        csvfile.write(csv_content)
    return csv_content


if __name__ == "__main__":
    engine = create_engine("mysql+mysqlconnector://root:admin@localhost/race_db3")
    query = "select * from race_db3.race_results where date > '2020-01-01' and pla REGEXP '^-?[0-9]+$' order by date,race_index"

    df = pd.read_sql(query, engine)
    print(df)
    all_races = [Race(row.to_dict()) for _, row in df.iterrows()]
    creator = HorseRacingVariableCreator()
    results = creator.compute_derived_variables(all_races)
    race_with_stats_to_csv(results)

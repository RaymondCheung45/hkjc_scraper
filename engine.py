from dataclasses import dataclass, field
from typing import Optional, List
from datetime import date
import csv
from collections import defaultdict


# Dataclass for races.csv
@dataclass
class Race:
    class_name: str
    course: str
    date: date
    distance: int
    going: str
    name: str
    prize: int
    race_index: int
    race_number: int
    racecourse: str
    rating: str
    sec1_time: Optional[float] = None
    sec2_time: Optional[float] = None
    sec3_time: Optional[float] = None
    sec4_time: Optional[float] = None
    sec5_time: Optional[float] = None
    sec6_time: Optional[float] = None
    track: str
    url: str
    results: List["Result"] = field(default_factory=list)  # List of related results
    sectimes: List["SecTime"] = field(default_factory=list)  # List of related sectional times


# Dataclass for results.csv
@dataclass
class Result:
    actual_weight: int
    date: date
    declar_horse_wt: int
    draw: int
    finish_time: float
    horse_id: str
    horse_name: str
    horse_number: int
    jockey: str
    jockey_id: str
    lbw: str  # Lengths behind winner, e.g., "-", "1/2"
    position: str  # e.g., "1", "10 DH"
    race_index: int
    race_number: int
    running_position: str  # e.g., "3 3 1"
    trainer: str
    trainer_id: str
    win_odds: float
    sectimes: List["SecTime"] = field(default_factory=list)  # List of related sectional times


# Dataclass for sectime.csv
@dataclass
class SecTime:
    date: date
    horse_code: str
    horse_id: str
    horse_name: str
    horse_number: int
    lbw: str
    position: int
    position_final: int
    race_number: int
    section: int
    subtime1: Optional[float] = None
    subtime2: Optional[float] = None
    time: float


# Function to load races from races.csv
def load_races(csv_path: str) -> List[Race]:
    races = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            race_date = date.fromisoformat(row["date"])
            sec1 = float(row["sec1_time"]) if row["sec1_time"] else None
            sec2 = float(row["sec2_time"]) if row["sec2_time"] else None
            sec3 = float(row["sec3_time"]) if row["sec3_time"] else None
            sec4 = float(row["sec4_time"]) if row["sec4_time"] else None
            sec5 = float(row["sec5_time"]) if row["sec5_time"] else None
            sec6 = float(row["sec6_time"]) if row["sec6_time"] else None
            race = Race(
                class_name=row["class_name"],
                course=row["course"],
                date=race_date,
                distance=int(row["distance"]),
                going=row["going"],
                name=row["name"],
                prize=int(row["prize"]),
                race_index=int(row["race_index"]),
                race_number=int(row["race_number"]),
                racecourse=row["racecourse"],
                rating=row["rating"],
                sec1_time=sec1,
                sec2_time=sec2,
                sec3_time=sec3,
                sec4_time=sec4,
                sec5_time=sec5,
                sec6_time=sec6,
                track=row["track"],
                url=row["url"],
            )
            races.append(race)
    return races


# Function to load results from results.csv
def load_results(csv_path: str) -> List[Result]:
    results = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            result_date = date.fromisoformat(row["date"])
            result = Result(
                actual_weight=int(row["actual_weight"]),
                date=result_date,
                declar_horse_wt=int(row["declar_horse_wt"]),
                draw=int(row["draw"]),
                finish_time=float(row["finish_time"]),
                horse_id=row["horse_id"],
                horse_name=row["horse_name"],
                horse_number=int(row["horse_number"]),
                jockey=row["jockey"],
                jockey_id=row["jockey_id"],
                lbw=row["lbw"],
                position=row["position"],
                race_index=int(row["race_index"]),
                race_number=int(row["race_number"]),
                running_position=row["running_position"],
                trainer=row["trainer"],
                trainer_id=row["trainer_id"],
                win_odds=float(row["win_odds"]),
            )
            results.append(result)
    return results


# Function to load sectional times from sectime.csv
def load_sectimes(csv_path: str) -> List[SecTime]:
    sectimes = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sectime_date = date.fromisoformat(row["date"])
            subtime1 = float(row["subtime1"]) if row["subtime1"] else None
            subtime2 = float(row["subtime2"]) if row["subtime2"] else None
            sectime = SecTime(
                date=sectime_date,
                horse_code=row["horse_code"],
                horse_id=row["horse_id"],
                horse_name=row["horse_name"],
                horse_number=int(row["horse_number"]),
                lbw=row["lbw"],
                position=int(row["position"]),
                position_final=int(row["position_final"]),
                race_number=int(row["race_number"]),
                section=int(row["section"]),
                subtime1=subtime1,
                subtime2=subtime2,
                time=float(row["time"]),
            )
            sectimes.append(sectime)
    return sectimes


# Function to link races, results, and sectimes
def link_data(races: List[Race], results: List[Result], sectimes: List[SecTime]) -> List[Race]:
    # Create lookup dictionaries for efficiency
    race_dict = {(r.date, r.race_number): r for r in races}
    result_dict = defaultdict(list)
    for result in results:
        result_dict[(result.date, result.race_number, result.horse_id)].append(result)

    # Link results to races
    for result in results:
        race_key = (result.date, result.race_number)
        if race_key in race_dict:
            race_dict[race_key].results.append(result)

    # Link sectimes to races and results
    for sectime in sectimes:
        race_key = (sectime.date, sectime.race_number)
        result_key = (sectime.date, sectime.race_number, sectime.horse_id)
        if race_key in race_dict:
            race_dict[race_key].sectimes.append(sectime)
            # Link sectime to corresponding result
            if result_key in result_dict:
                for result in result_dict[result_key]:
                    result.sectimes.append(sectime)

    return list(race_dict.values())


# Example usage
def main():
    # Load data from CSV files
    races = load_races("races.csv")
    results = load_results("results.csv")
    sectimes = load_sectimes("sectime.csv")

    # Link the data
    linked_races = link_data(races, results, sectimes)

    # Example: Print details of the first race with its results and sectional times
    if linked_races:
        race = linked_races[0]
        print(f"Race: {race.name} on {race.date} (Race Number: {race.race_number})")
        print("Results:")
        for result in race.results:
            print(f"  Horse: {result.horse_name}, Position: {result.position}, Finish Time: {result.finish_time}")
            print("  Sectional Times:")
            for sectime in result.sectimes:
                print(f"    Section {sectime.section}: Time {sectime.time}, Subtime1 {sectime.subtime1}, Subtime2 {sectime.subtime2}")
        print("Race Sectional Times:")
        for sectime in race.sectimes:
            print(f"  Horse: {sectime.horse_name}, Section {sectime.section}, Time: {sectime.time}")


if __name__ == "__main__":
    main()

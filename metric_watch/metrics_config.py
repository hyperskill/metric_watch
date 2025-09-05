from dataclasses import dataclass, field
from typing import List, Optional, TextIO
import yaml


@dataclass(frozen=True)
class Slice:
    name: str
    values: List[str]


@dataclass(frozen=True)
class Granularity:
    name: str
    window_size: int
    sigma_n: int
    slices: List[Slice] = field(default_factory=list)


@dataclass(frozen=True)
class DateRange:
    start: str = ""
    end: str = "now()"


@dataclass(frozen=True)
class Metric:
    name: str
    description: str
    granularities: List[Granularity]
    depends_on: List[str] = field(default_factory=list)
    formula: Optional[str] = None
    date_range: DateRange = DateRange()

    def __hash__(self):
        return hash(self.name)


@dataclass(frozen=True)
class MetricsConfig:
    metrics: List[Metric]

    @staticmethod
    def from_yaml(yaml_content: str | TextIO) -> "MetricsConfig":
        try:
            yaml_data = yaml.safe_load(yaml_content)
            metrics_list = []

            for metric in yaml_data["metrics"]:
                granularities = []

                for granularity in metric["granularities"]:
                    for granularity_name, granularity_values in granularity.items():
                        slices = []
                        if "slices" in granularity_values:
                            for slice in granularity_values["slices"]:
                                for slice_name, slice_values in slice.items():
                                    slices.append(Slice(slice_name, slice_values))
                        granularities.append(
                            Granularity(
                                name=granularity_name,
                                window_size=granularity_values["window_size"],
                                sigma_n=granularity_values["sigma_n"],
                                slices=slices,
                            )
                        )

                metric_instance = Metric(
                    name=metric["name"],
                    description=metric["description"],
                    depends_on=metric.get("depends_on", []),
                    formula=metric.get("formula", None),
                    granularities=granularities,
                )
                metrics_list.append(metric_instance)

            return MetricsConfig(metrics=metrics_list)
        except yaml.YAMLError as e:
            print(f"Error parsing YAML: {e}")
            return MetricsConfig(metrics=[])


# Example usage
if __name__ == "__main__":
    from constants import PATH_TO_METRIC_HIERARCHY  # type: ignore

    with open(PATH_TO_METRIC_HIERARCHY, "r") as file:
        config = MetricsConfig.from_yaml(file.read())
        print(config)

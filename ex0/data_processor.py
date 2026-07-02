#!/Library/Frameworks/Python.framework/Versions/3.12/bin/python3

from abc import ABC, abstractmethod
from typing import Any


class DataProcessor(ABC):

    def __init__(self) -> None:
        self.ingested_data: list[tuple[int, str]] = []
        self.idx = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        if not self.validate(data):
            raise Exception(
                "ERROR: Data is invalid. Please validate before ingestion.")

    def output(self) -> tuple[int, str]:
        tuple_out = self.ingested_data[0]
        self.ingested_data.pop(0)
        print(tuple_out)
        return tuple_out


class NumericProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, (int, float)):
            return True
        elif isinstance(data, list) and all(
             isinstance(d, (int, float)) for d in data):
            return True
        else:
            return False

    def ingest(self, data: int | float | list) -> None:
        super().ingest(data)
        if isinstance(data, (int, float)):
            self.ingested_data.append((self.idx, str(data)))
            self.idx += 1
        else:
            for d in data:
                self.ingested_data.append((self.idx, str(d)))
                self.idx += 1


class TextProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        elif isinstance(data, list) and all(isinstance(d, str) for d in data):
            return True
        else:
            return False

    def ingest(self, data: str | list[str]) -> None:
        super().ingest(data)
        if isinstance(data, str):
            self.ingested_data.append((self.idx, data))
            self.idx += 1
        else:
            for d in data:
                self.ingested_data.append((self.idx, d))
                self.idx += 1


class LogProcessor(DataProcessor):

    def validate(self, data: Any) -> bool:
        if isinstance(data, dict) and all(
            isinstance(k, str) and isinstance(v, str) for k, v in data.items()
              ):
            return True
        if isinstance(data, list):
            for d in data:
                if isinstance(d, dict) and all(
                        isinstance(k, str) and isinstance(v, str)
                        for k, v in d.items()
                      ):
                    return True
        return False

    def ingest(self, data: dict[str, str] | list[dict[str, str]]) -> None:
        super().ingest(data)
        if isinstance(data, dict):
            self.ingested_data.append((self.idx, ": ".join(data.values())))
            self.idx += 1
        else:
            for d in data:
                self.ingested_data.append((self.idx, ": ".join(d.values())))
                self.idx += 1


if __name__ == "__main__":
    print("======= DATA PROCESSING =======\n")
    num_p = NumericProcessor()
    text_p = TextProcessor()
    log_p = LogProcessor()
    print("\n======= NumericProcessor\n")
    nums = list[int | float]
    for data in [12.9, 10, 123, "hello"]:
        print(f"validating '{data}':{num_p.validate(data)}")
        try:
            num_p.ingest(data)  # error because of str
        except Exception as e:
            print(f"Got Exception: {e}")
    num_p.output()
    num_p.output()
    num_p.output()
    print("\n======= TextProcessor\n")
    data_text = ["hi", "brother"]
    print(f"validating '{data_text}':{text_p.validate(data_text)}")
    text_p.ingest(data_text)
    try:
        text_p.ingest(1234)  # error because of int
    except Exception as e:
        print(f"Got Exception: {e}")
    text_p.output()
    text_p.output()

    print("\n======= LogProcessor\n")
    data = [{"login": "user123", "password": "password123"}, {"whats": "good"}]
    print(f"validating '{data}':{log_p.validate(data)}")
    try:
        log_p.ingest(data)
    except Exception as e:
        print(f"Got Exception: {e}")
    log_p.output()
    log_p.output()

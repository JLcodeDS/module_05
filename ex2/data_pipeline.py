#!/Library/Frameworks/Python.framework/Versions/3.12/bin/python3

from typing import Any, Protocol
from abc import ABC, abstractmethod


class DataProcessor(ABC):

    def __init__(self) -> None:
        self.ingested_data: list[tuple[int, str]] = []
        self.idx = 0
        self.name = "Data Processor"

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
        return tuple_out


class NumericProcessor(DataProcessor):

    def __init__(self) -> None:
        super().__init__()
        self.name = "Numeric Processor"

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
    def __init__(self) -> None:
        super().__init__()
        self.name = "Text Processor"

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
    def __init__(self) -> None:
        super().__init__()
        self.name = "Log Processor"

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


class ExportPlugin(Protocol):
    def process_output(self, data: list[tuple[int, str]]) -> None: ...


class CSVPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        output_str = ""
        for element in data:
            output_str += element[1] + ","
        output_str = output_str[:-1]
        print(f"CSV Output:\n{output_str}")


class JSONPlugin:
    def process_output(self, data: list[tuple[int, str]]) -> None:
        output_str = "{"
        for element in data:
            output_str += f'"item_{element[0]}": "{element[1]}"' + ", "
        output_str = output_str[:-1] + "}"
        print(f"JSON Output:\n{output_str}")


class DataStream:
    def __init__(self) -> None:
        print("Initialize Data Stream...")
        self.processors: list[DataProcessor] = []
        # self.items_processed: dict[str, in]

    def register_processor(self, proc: DataProcessor) -> None:
        self.processors.append(proc)
        print(f"Registering {proc.name}\n")

    def process_stream(self, stream: list[Any]) -> None:
        for element in stream:
            validated = 0
            for proc in iter(self.processors):
                if proc.validate(element):
                    proc.ingest(element)
                    validated = 1
            if not validated:
                print("DataStream Error - " +
                      f"Can't process element in stream: {element}")

    def print_processors_stats(self) -> None:
        print("=== DATA STREAM STATISTICS ===")
        if len(self.processors) == 0:
            print("No processor found, no data")
        else:
            for proc in iter(self.processors):
                print(
                    f"{proc.name}: total {proc.idx}, " +
                    f"remaining {len(proc.ingested_data)} on processor")
        print()

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        for proc in iter(self.processors):
            data: list[tuple[int, str]] = []
            for n in range(nb):
                if len(proc.ingested_data):
                    data.append(proc.output())
            plugin.process_output(data)


if __name__ == "__main__":
    test_stream = ['Hello world', [3.14, -1, 2.71],
                   [
                    {'log_level': 'WARNING',
                     'log_message': 'Telnet access! Use ssh instead'},
                    {'log_level': 'INFO',
                        'log_message': 'User wil is connected'}
                   ], 42, ['Hi', 'five']]
    test_stream_2 = [21, ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
                     [
                    {'log_level': 'ERROR', 'log_message': '500 server crash'},
                    {'log_level': 'NOTICE',
                     'log_message': 'Certificate expires in 10 days'}
                     ], [32, 42, 64, 84, 128, 168], 'World hello']
    print("=== Code Nexus - Data Pipeline ===\n")
    datastream = DataStream()
    datastream.print_processors_stats()
    num_p = NumericProcessor()
    text_p = TextProcessor()
    log_p = LogProcessor()
    datastream.register_processor(num_p)
    datastream.register_processor(text_p)
    datastream.register_processor(log_p)
    print(f"Sending first batch of data on stream: {test_stream}")
    datastream.process_stream(test_stream)
    datastream.print_processors_stats()
    print("Send 3 processed data from each processor to a CSV plugin:")
    datastream.output_pipeline(3, CSVPlugin())
    datastream.print_processors_stats()
    print(f"Send another batch of data: {test_stream_2}")
    datastream.process_stream(test_stream_2)
    datastream.print_processors_stats()
    print("Send 5 processed data from each processor to a JSON plugin:")
    datastream.output_pipeline(5, JSONPlugin())
    datastream.print_processors_stats()

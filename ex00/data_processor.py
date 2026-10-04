from abc import ABC, abstractmethod
from typing import Any, Union


class DataProcessor(ABC):
    """Classe base abstrata para processadores de dados."""

    def __init__(self) -> None:
        # Armazenamos uma lista de tuplas (rank, dado_formatado)
        self.storage: list[tuple[int, str]] = []
        self.rank_counter: int = 0

    @abstractmethod
    def validate(self, data: Any) -> bool:
        """Verifica se os dados são apropriados para o processador."""
        pass

    @abstractmethod
    def ingest(self, data: Any) -> None:
        """Processa e armazena os dados internamente."""
        pass

    def output(self) -> tuple[int, str]:
        """Extrai a peça de dado mais antiga junto com seu rank."""
        if not self.storage:
            raise IndexError("No data remaining in processor.")
        return self.storage.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        # Booleanos herdam de int em Python, então precisamos barrá-los explicitamente
        if isinstance(data, bool):
            return False
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list) and len(data) > 0:
            return all(
                isinstance(x, (int, float)) and not isinstance(x, bool)
                for x in data
            )
        return False

    def ingest(self, data: Union[int, float, list[Union[int, float]]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper numeric data")

        items = data if isinstance(data, list) else [data]
        for item in items:
            self.storage.append((self.rank_counter, str(item)))
            self.rank_counter += 1


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, str):
            return True
        if isinstance(data, list) and len(data) > 0:
            return all(isinstance(x, str) for x in data)
        return False

    def ingest(self, data: Union[str, list[str]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper text data")

        items = data if isinstance(data, list) else [data]
        for item in items:
            self.storage.append((self.rank_counter, item))
            self.rank_counter += 1


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        def is_valid_dict(d: Any) -> bool:
            return isinstance(d, dict) and all(
                isinstance(k, str) and isinstance(v, str)
                for k, v in d.items()
            )

        if is_valid_dict(data):
            return True
        if isinstance(data, list) and len(data) > 0:
            return all(is_valid_dict(x) for x in data)
        return False

    def ingest(self, data: Union[dict[str, str], list[dict[str, str]]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        def format_log(d: dict[str, str]) -> str:
            if "log_level" in d and "log_message" in d:
                return f"{d['log_level']}: {d['log_message']}"
            return ": ".join(f"{k}: {v}" for k, v in d.items())

        items = data if isinstance(data, list) else [data]
        for item in items:
            self.storage.append((self.rank_counter, format_log(item)))
            self.rank_counter += 1


if __name__ == "__main__":
    print("=== Code Nexus - Data Processor ===")

    print("\nTesting Numeric Processor...")
    num_proc = NumericProcessor()
    print(f"Trying to validate input '42': {num_proc.validate('42')}")
    print(f"Trying to validate input 'Hello': {num_proc.validate('Hello')}")

    print("Test invalid ingestion of string 'foo' without prior validation:")
    try:
        # A linha abaixo vai gerar um "Warning" proposital no Mypy, 
        # exatamente como exigido no PDF da lista para testes de exceção.
        num_proc.ingest("foo")  # type: ignore
    except Exception as e:
        print(f"Got exception: {e}")

    test_nums = [1, 2, 3, 4, 5]
    print(f"Processing data: {test_nums}")
    num_proc.ingest(test_nums)
    print("Extracting 3 values...")
    for _ in range(3):
        rank, val = num_proc.output()
        print(f"Numeric value {rank}: {val}")

    print("\nTesting Text Processor...")
    txt_proc = TextProcessor()
    print(f"Trying to validate input '42': {txt_proc.validate(42)}")
    test_texts = ['Hello', 'Nexus', 'World']
    print(f"Processing data: {test_texts}")
    txt_proc.ingest(test_texts)
    print("Extracting 1 value...")
    rank, val = txt_proc.output()
    print(f"Text value {rank}: {val}")

    print("\nTesting Log Processor...")
    log_proc = LogProcessor()
    print(f"Trying to validate input 'Hello': {log_proc.validate('Hello')}")
    test_logs = [
        {'log_level': 'NOTICE', 'log_message': 'Connection to server'},
        {'log_level': 'ERROR', 'log_message': 'Unauthorized access!!'}
    ]
    print(f"Processing data: {test_logs}")
    log_proc.ingest(test_logs)
    print("Extracting 2 values...")
    for _ in range(2):
        rank, val = log_proc.output()
        print(f"Log entry {rank}: {val}")

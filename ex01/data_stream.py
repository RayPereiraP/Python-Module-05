import abc
import typing
from typing import Any, Union, List, Dict


# ==========================================
# CÓDIGO BASE DO EXERCÍCIO 0
# ==========================================

class DataProcessor(abc.ABC):
    """Classe base abstrata para processadores de dados."""

    def __init__(self) -> None:
        self.storage: List[tuple[int, str]] = []
        self.rank_counter: int = 0

    @abc.abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    @abc.abstractmethod
    def ingest(self, data: Any) -> None:
        pass

    def output(self) -> tuple[int, str]:
        if not self.storage:
            raise IndexError("No data remaining in processor.")
        return self.storage.pop(0)


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, bool):
            return False
        if isinstance(data, (int, float)):
            return True
        if isinstance(data, list):
            if not data:
                return False
            return all(
                isinstance(x, (int, float)) and not isinstance(x, bool)
                for x in data
            )
        return False

    def ingest(self, data: Union[int, float, List[Union[int, float]]]) -> None:
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
        if isinstance(data, list):
            if not data:
                return False
            return all(isinstance(x, str) for x in data)
        return False

    def ingest(self, data: Union[str, List[str]]) -> None:
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
        if isinstance(data, list):
            if not data:
                return False
            return all(is_valid_dict(x) for x in data)
        return False

    def ingest(self, data: Union[Dict[str, str], List[Dict[str, str]]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        def format_log(d: Dict[str, str]) -> str:
            if "log_level" in d and "log_message" in d:
                return f"{d['log_level']}: {d['log_message']}"
            return str(d)

        items = data if isinstance(data, list) else [data]
        for item in items:
            self.storage.append((self.rank_counter, format_log(item)))
            self.rank_counter += 1


# ==========================================
# CÓDIGO NOVO DO EXERCÍCIO 1
# ==========================================

class DataStream:
    """Gerencia o fluxo e o roteamento polimórfico de dados."""

    def __init__(self) -> None:
        self.processors: List[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        """Registra um novo processador de dados."""
        if proc not in self.processors:
            self.processors.append(proc)

    def process_stream(self, stream: List[typing.Any]) -> None:
        """Envia cada elemento para o processador adequado usando polimorfismo."""
        for item in stream:
            handled = False
            for proc in self.processors:
                # Polimorfismo em ação: não importa qual é a classe filha,
                # chamamos o contrato abstrato validate() e ingest()
                if proc.validate(item):
                    proc.ingest(item)
                    handled = True
                    break
            
            # Imprime erro se nenhum processador aceitar o dado
            if not handled:
                print(
                    "DataStream error - Can't process element in stream: "
                    f"{item}"
                )

    def print_processors_stats(self) -> None:
        """Imprime as estatísticas de cada processador registrado."""
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
            return
        
        for proc in self.processors:
            # Formata o nome da classe (ex: "NumericProcessor" -> "Numeric Processor")
            class_name = proc.__class__.__name__
            name = class_name.replace("Processor", " Processor")
            
            # total_processed = rank_counter (todos que já entraram)
            # remaining = len(storage) (os que ainda não foram retirados pelo output)
            total = proc.rank_counter
            remaining = len(proc.storage)
            print(f"{name}: total {total} items processed, "
                  f"remaining {remaining} on processor")


# ==========================================
# TESTES REQUERIDOS (MAIN)
# ==========================================

def main() -> None:
    print("=== Code Nexus - Data Stream ===")
    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Numeric Processor")
    num_proc = NumericProcessor()
    stream.register_processor(num_proc)

    batch = [
        'Hello world',
        [3.14, -1, 2.71],
        [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five']
    ]

    print(f"Send first batch of data on stream: {batch}")
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nRegistering other data processors")
    print("Send the same batch again")
    txt_proc = TextProcessor()
    log_proc = LogProcessor()
    
    stream.register_processor(txt_proc)
    stream.register_processor(log_proc)
    
    stream.process_stream(batch)
    stream.print_processors_stats()

    print("\nConsume some elements from the data processors: "
          "Numeric 3, Text 2, Log 1")
    for _ in range(3):
        num_proc.output()
    for _ in range(2):
        txt_proc.output()
    for _ in range(1):
        log_proc.output()
        
    stream.print_processors_stats()


if __name__ == "__main__":
    main()

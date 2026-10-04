import abc
from typing import Any, Union, Protocol


# ==========================================
# CÓDIGO BASE (EX0 e EX1)
# ==========================================

class DataProcessor(abc.ABC):
    def __init__(self) -> None:
        self.storage: list[tuple[int, str]] = []
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
            return bool(data) and all(
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
        if isinstance(data, list):
            return bool(data) and all(isinstance(x, str) for x in data)
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
        if isinstance(data, list):
            return bool(data) and all(is_valid_dict(x) for x in data)
        return False

    def ingest(self, data: Union[dict[str, str], list[dict[str, str]]]) -> None:
        if not self.validate(data):
            raise ValueError("Improper log data")

        def format_log(d: dict[str, str]) -> str:
            if "log_level" in d and "log_message" in d:
                return f"{d['log_level']}: {d['log_message']}"
            return str(d)

        items = data if isinstance(data, list) else [data]
        for item in items:
            self.storage.append((self.rank_counter, format_log(item)))
            self.rank_counter += 1


# ==========================================
# CÓDIGO NOVO (EX2) - PROTOCOLOS E PLUGINS
# ==========================================

class ExportPlugin(Protocol):
    """Protocolo (Duck Typing) para garantir a assinatura correta dos plugins."""
    def process_output(self, data: list[tuple[int, str]]) -> None:
        ...


class CSVPlugin:
    """Plugin que exporta os dados formatados como CSV manualmente."""
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        # Extrai apenas o valor ignorando o rank e junta com vírgulas
        values = [item[1] for item in data]
        print(f"CSV Output: {','.join(values)}")


class JSONPlugin:
    """Plugin que exporta os dados formatados como JSON manualmente."""
    def process_output(self, data: list[tuple[int, str]]) -> None:
        if not data:
            return
        # Constrói os pares chave-valor "item_RANK": "VALOR"
        json_pairs = []
        for rank, val in data:
            json_pairs.append(f'"item_{rank}": "{val}"')
        
        # Junta tudo envelopado em chaves
        print(f"JSON Output: {{{', '.join(json_pairs)}}}")


class DataStream:
    """Gerenciador de fluxo polimórfico e de pipeline."""
    
    def __init__(self) -> None:
        self.processors: list[DataProcessor] = []

    def register_processor(self, proc: DataProcessor) -> None:
        if proc not in self.processors:
            self.processors.append(proc)

    def process_stream(self, stream: list[Any]) -> None:
        for item in stream:
            handled = False
            for proc in self.processors:
                if proc.validate(item):
                    proc.ingest(item)
                    handled = True
                    break
            # Esconde o print de erro no Ex2 para focar apenas nas estatísticas 
            # e saídas, ou mantém se quiser acompanhar. O PDF de Ex2 não mostra 
            # erros na saída de exemplo, então passamos silenciosamente.
            if not handled:
                pass

    def print_processors_stats(self) -> None:
        print("== DataStream statistics ==")
        if not self.processors:
            print("No processor found, no data")
            return
        for proc in self.processors:
            class_name = proc.__class__.__name__
            name = class_name.replace("Processor", " Processor")
            print(f"{name}: total {proc.rank_counter} items processed, "
                  f"remaining {len(proc.storage)} on processor")

    def output_pipeline(self, nb: int, plugin: ExportPlugin) -> None:
        """Extrai até 'nb' elementos de CADA processador e envia ao plugin."""
        for proc in self.processors:
            extracted_data = []
            for _ in range(nb):
                try:
                    # Extrai o mais antigo. Se a lista esvaziar, cai no IndexError
                    extracted_data.append(proc.output())
                except IndexError:
                    break
            
            # Se extraiu algo deste processador, delega ao plugin de exportação
            if extracted_data:
                plugin.process_output(extracted_data)


# ==========================================
# TESTES REQUERIDOS (MAIN) - BASEADO NO PDF
# ==========================================

def main() -> None:
    print("=== Code Nexus - Data Pipeline ===")
    print("Initialize Data Stream...")
    stream = DataStream()
    stream.print_processors_stats()

    print("\nRegistering Processors")
    num_proc = NumericProcessor()
    txt_proc = TextProcessor()
    log_proc = LogProcessor()
    
    stream.register_processor(num_proc)
    stream.register_processor(txt_proc)
    stream.register_processor(log_proc)

    batch1 = [
        'Hello world',
        [3.14, -1, 2.71],
        [{'log_level': 'WARNING', 'log_message': 'Telnet access! Use ssh instead'},
         {'log_level': 'INFO', 'log_message': 'User wil is connected'}],
        42,
        ['Hi', 'five']
    ]
    print(f"\nSend first batch of data on stream: {batch1}")
    stream.process_stream(batch1)
    stream.print_processors_stats()

    print("\nSend 3 processed data from each processor to a CSV plugin:")
    csv_plugin = CSVPlugin()
    stream.output_pipeline(3, csv_plugin)
    print()
    stream.print_processors_stats()

    batch2 = [
        21,
        ['I love AI', 'LLMs are wonderful', 'Stay healthy'],
        [{'log_level': 'ERROR', 'log_message': '500 server crash'},
         {'log_level': 'NOTICE', 'log_message': 'Certificate expires in 10 days'}],
        [32, 42, 64, 84, 128, 168],
        'World hello'
    ]
    print(f"\nSend another batch of data: {batch2}")
    stream.process_stream(batch2)
    stream.print_processors_stats()

    print("\nSend 5 processed data from each processor to a JSON plugin:")
    json_plugin = JSONPlugin()
    stream.output_pipeline(5, json_plugin)
    print()
    stream.print_processors_stats()


if __name__ == "__main__":
    main()

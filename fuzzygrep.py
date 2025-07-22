import json
import csv
from rapidfuzz import process, fuzz
from pathlib import Path
import typer
from typing import Optional
from rich.tree import Tree
from rich.table import Table
from rich import print
from rich.console import Console
from prompt_toolkit import PromptSession
from prompt_toolkit.completion import Completer, Completion
from prompt_toolkit.key_binding import KeyBindings
import os
import subprocess
import collections
import tempfile
import logging
import sys
import atexit
import termgraph.termgraph as termgraph

app = typer.Typer()

# Setup logging
log_file_path = None
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a temporary log file
_temp_log_file_path = tempfile.NamedTemporaryFile(delete=False, mode='w', suffix='.log').name
file_handler = logging.FileHandler(_temp_log_file_path)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

# Create console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter('%(levelname)s - %(message)s'))
logger.addHandler(console_handler)

_verbose_output_enabled = False

@atexit.register
def cleanup_log_file():
    if _temp_log_file_path and os.path.exists(_temp_log_file_path):
        if _verbose_output_enabled:
            with open(_temp_log_file_path, 'r') as f:
                print("\n--- Log File Content ---")
                print(f.read())
                print("------------------------")
        os.remove(_temp_log_file_path)
        logger.info(f"Cleaned up temporary log file: {_temp_log_file_path}")


class FuzzyJSONSearcher:

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.data = None
        self.unique_keys = []
        self._allowed_keys_filter = [] # New attribute for key filtering
        self._load_data()

    def _load_data(self):
        ext = self.file_path.suffix.lower()
        if ext == '.json':
            self.data = self._load_json()
            if self.data:
                self._all_keys = self._extract_json_keys(self.data)
                self._all_values = self._extract_json_values(self.data)
        elif ext == '.csv':
            self.data = self._load_csv()
            if self.data:
                self._all_keys = list(self.data[0].keys()) if self.data else []
                self._all_values = [] # CSVs don't have nested values in the same way
        self._apply_key_filter()

    def _apply_key_filter(self):
        if not self._allowed_keys_filter:
            self.unique_keys = sorted(list(set(self._all_keys)))
            self.unique_values = sorted(list(set(self._all_values)))
            self.value_to_keys_map = self._build_value_to_key_map(self.data)
        else:
            filtered_keys = []
            for key in self._all_keys:
                for pattern in self._allowed_keys_filter:
                    if pattern in key: # Simple substring match for now
                        filtered_keys.append(key)
                        break
            self.unique_keys = sorted(list(set(filtered_keys)))

            # Rebuild value_to_keys_map and unique_values based on filtered keys
            filtered_value_to_keys_map = {}
            for val, keys in self._build_value_to_key_map(self.data).items():
                filtered_associated_keys = [k for k in keys if k in self.unique_keys]
                if filtered_associated_keys:
                    filtered_value_to_keys_map[val] = filtered_associated_keys
            self.value_to_keys_map = filtered_value_to_keys_map
            self.unique_values = sorted(list(filtered_value_to_keys_map.keys()))

    def _load_json(self):
        """Loads a JSON file with error handling."""
        try:
            with open(self.file_path, 'r') as f:
                data = json.load(f)
            return data
        except FileNotFoundError:
            logger.error(f"File not found at '{self.file_path}'")
            return None
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON format in '{self.file_path}'")
            return None

    def _load_csv(self):
        """Loads a CSV file with error handling."""
        try:
            with open(self.file_path, newline='') as f:
                reader = csv.DictReader(f)
                return list(reader)
        except FileNotFoundError:
            logger.error(f"File not found at '{self.file_path}'")
            return None
        except Exception as e:
            logger.error(f"Error reading CSV file '{self.file_path}': {e}")
            return None

    def _extract_json_keys(self, data, prefix=""):
        """Recursively extracts all keys from a nested JSON object, creating dot-notation paths."""
        keys = []
        if isinstance(data, dict):
            for k, v in data.items():
                full_key = f"{prefix}.{k}" if prefix else k
                keys.append(full_key)
                keys.extend(self._extract_json_keys(v, prefix=full_key))
        elif isinstance(data, list):
            for item in data:
                keys.extend(self._extract_json_keys(item, prefix=prefix))
        return keys

    def _extract_json_values(self, data):
        """Recursively extracts all values from a nested JSON object."""
        values = []
        if isinstance(data, dict):
            for k, v in data.items():
                if isinstance(v, (dict, list)):
                    values.extend(self._extract_json_values(v))
                else:
                    values.append(str(v))
        elif isinstance(data, list):
            for item in data:
                values.extend(self._extract_json_values(item))
        return values

    def _get_values_by_path(self, data, path):
        """Recursively retrieves all values for a given path from nested data."""
        values = []
        if isinstance(data, list):
            for item in data:
                values.extend(self._get_values_by_path(item, path))
            return values

        keys = path.split('.')
        value = data
        for key in keys:
            try:
                if isinstance(value, dict):
                    value = value[key]
                elif isinstance(value, list):
                    # This case is handled by the initial list check
                    return []
                else:
                    return []
            except (KeyError, TypeError, IndexError):
                return []
        return [value]

    def _build_value_to_key_map(self, data, prefix=""):
        """Recursively builds a map from values to the keys that contain them."""
        value_to_keys = {}
        if isinstance(data, dict):
            for k, v in data.items():
                full_key = f"{prefix}.{k}" if prefix else k
                if isinstance(v, (dict, list)):
                    nested_map = self._build_value_to_key_map(v, prefix=full_key)
                    for val, keys in nested_map.items():
                        if val not in value_to_keys:
                            value_to_keys[val] = set()
                        value_to_keys[val].update(keys)
                else:
                    val_str = str(v)
                    if val_str not in value_to_keys:
                        value_to_keys[val_str] = set()
                    value_to_keys[val_str].add(full_key)
        elif isinstance(data, list):
            for item in data:
                nested_map = self._build_value_to_key_map(item, prefix=prefix)
                for val, keys in nested_map.items():
                    if val not in value_to_keys:
                        value_to_keys[val] = set()
                    value_to_keys[val].update(keys)

        return {val: sorted(list(keys)) for val, keys in value_to_keys.items()}

    def fuzzy_search(self, query: str, limit: int = 10, search_type: str = "keys"):
        """Finds close matches for a query from a list of candidates using fuzzywuzzy."""
        if search_type == "keys":
            candidates = self.unique_keys
        else: # search_type == "values"
            candidates = self.unique_values
        matches = process.extract(query, candidates, scorer=fuzz.WRatio, limit=limit, score_cutoff=60)

        cleaned = [(m[0], m[1]) for m in matches]
        return cleaned

    def display_matches(self, matches, search_type: str = "keys"):
        if not matches:
            logger.info("No matches found.")
            return

        console = Console()
        
        table = Table(title=f"Fuzzy Search Results ({len(matches)} total)")
        if search_type == "keys":
            table.add_column("Key", style="cyan")
            table.add_column("Value", style="magenta")
        else: # search_type == "values"
            table.add_column("Value", style="magenta")
            table.add_column("Keys", style="cyan")
        table.add_column("Score", style="green")

        nested_matches = []

        for m, score in matches:
            if search_type == "keys":
                values = self._get_values_by_path(self.data, m)
                if values:
                    is_nested = any(isinstance(v, (dict, list)) for v in values)
                    if is_nested:
                        nested_matches.append((m, score, values))
                    else:
                        for value in values:
                            table.add_row(m, str(value), str(score))
            else: # search_type == "values"
                keys = self.value_to_keys_map.get(m, [])
                if keys:
                    table.add_row(m, ", ".join(keys), str(score))

        if table.rows:
            if len(matches) > 20: # Threshold for pagination
                with console.pager():
                    console.print(table)
            else:
                console.print(table)

        if nested_matches and search_type == "keys":
            for m, score, values in nested_matches:
                tree = Tree(f"Values for '{m}' (Score: {score})")
                for value in values:
                    if isinstance(value, (dict, list)):
                        generate_rich_tree(value, parent_tree=tree, name=m)
                    else:
                        tree.add(str(value))
                console.print(tree)

def generate_rich_tree(data, parent_tree=None, name="root"):
    if parent_tree is None:
        tree = Tree(name)
    else:
        tree = parent_tree.add(name)

    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                generate_rich_tree(value, parent_tree=tree, name=str(key))
            else:
                tree.add(f"{key}: [green]{value}[/green]")
    elif isinstance(data, list):
        for i, item in enumerate(data[:5]):  # Limit to first 5 items for brevity
            if isinstance(item, (dict, list)):
                generate_rich_tree(item, parent_tree=tree, name=f"[{i}]")
            else:
                tree.add(f"- [green]{item}[/green]")
    return tree

class FuzzyCompleter(Completer):
    def __init__(self, searcher: "FuzzyJSONSearcher", completion_type: str = "keys"):
        self.searcher = searcher
        self.completion_type = completion_type # 'keys' or 'values'

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        if text:
            if self.completion_type == "keys":
                candidates = self.searcher.unique_keys
            else: # completion_type == "values"
                candidates = self.searcher.unique_values

            matches = process.extract(text, candidates, scorer=fuzz.WRatio, limit=5, score_cutoff=60)
            cleaned = [(m[0], m[1]) for m in matches]
            for m, score in cleaned:
                yield Completion(m, start_position=-len(text))

class CommandCompleter(Completer):
    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        if text.startswith('/'):
            commands = ["/exit", "/load", "/open", "/only"]
            for cmd in commands:
                if cmd.startswith(text):
                    yield Completion(cmd, start_position=-len(text))

class DynamicCompleter(Completer):
    def __init__(self, fuzzy_completer, command_completer):
        self.fuzzy_completer = fuzzy_completer
        self.command_completer = command_completer

    def get_completions(self, document, complete_event):
        text = document.text_before_cursor
        if text.startswith('/'):
            yield from self.command_completer.get_completions(document, complete_event)
        else:
            yield from self.fuzzy_completer.get_completions(document, complete_event)

@app.command()
def main(
    file_path: Path = typer.Argument(..., help="The path to the .json or .csv file to inspect."),
    chart: bool = typer.Option(False, "--chart", help="Display a tree chart of the file structure and exit."),
    histogram: bool = typer.Option(False, "--histogram", help="Display a histogram of key and value counts and exit."),
    verbose: bool = typer.Option(False, "--verbose", help="Enable verbose logging output at the end of the program.")
):
    """Fuzzy search for keys/columns in JSON or CSV files."""
    global _verbose_output_enabled
    _verbose_output_enabled = verbose

    logger.info(f"Log file located at: {_temp_log_file_path}")

    searcher = FuzzyJSONSearcher(file_path)

    if chart:
        if searcher.data:
            tree = generate_rich_tree(searcher.data, name=str(file_path))
            print(tree)
        else:
            logger.error("Could not generate chart as data failed to load.")
        raise typer.Exit()

    if histogram:
        if searcher.data:
            key_counts = collections.Counter(searcher._all_keys)
            value_counts = collections.Counter(searcher._all_values)

            logger.info("\nKey Histogram:")
            if key_counts:
                with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_key_file:
                    for k, v in key_counts.most_common(20):
                        temp_key_file.write(f"{k},{v}\n")
                try:
                    subprocess.run(["termgraph", temp_key_file.name, "--width", "50", "--format", "{:<5.2f}", "--suffix", "", "--color", "blue"], check=True)
                except subprocess.CalledProcessError as e:
                    logger.error(f"Error generating key histogram: {e}")
                finally:
                    os.remove(temp_key_file.name)
            else:
                logger.info("No keys found for histogram.")

            logger.info("\nValue Histogram:")
            if value_counts:
                with tempfile.NamedTemporaryFile(mode='w+', delete=False) as temp_value_file:
                    for k, v in value_counts.most_common(20):
                        temp_value_file.write(f"{k},{v}\n")
                try:
                    subprocess.run(["termgraph", temp_value_file.name, "--width", "50", "--format", "{:<5.2f}", "--suffix", "", "--color", "green"], check=True)
                except subprocess.CalledProcessError as e:
                    logger.error(f"Error generating value histogram: {e}")
                finally:
                    os.remove(temp_value_file.name)
            else:
                logger.info("No values found for histogram.")
        else:
            logger.error("Could not generate histogram as data failed to load.")
        raise typer.Exit()

    logger.info("Press Ctrl+T to toggle autocompletion. Press Ctrl+V to toggle value autocompletion. Press Ctrl+C to exit.")

    kb = KeyBindings()
    current_completion_type = "keys"

    @kb.add('c-t')
    def _(event):
        """Toggle autocompletion."""
        session.complete_while_typing = not session.complete_while_typing

    @kb.add('c-v')
    def _(event):
        """Toggle autocompletion between keys and values."""
        nonlocal current_completion_type
        if current_completion_type == "keys":
            current_completion_type = "values"
            logger.info("Autocompletion set to values.")
        else:
            current_completion_type = "keys"
            logger.info("Autocompletion set to keys.")
        dynamic_completer.fuzzy_completer.completion_type = current_completion_type

    fuzzy_completer = FuzzyCompleter(searcher, completion_type=current_completion_type)
    command_completer = CommandCompleter()
    dynamic_completer = DynamicCompleter(fuzzy_completer, command_completer)

    session = PromptSession(completer=dynamic_completer, complete_while_typing=True, key_bindings=kb)

    while True:
        try:
            query = session.prompt(f"\n[{file_path.name}] Search> ")
            if not query:
                logger.info("Empty query, continuing...")
                continue

            if query.startswith('/'):
                if query == '/exit':
                    logger.info("Exiting.")
                    break
                elif query.startswith('/load'):
                    parts = query.split()
                    if len(parts) > 1:
                        new_file_path = Path(parts[1])
                        if new_file_path.exists():
                            searcher = FuzzyJSONSearcher(new_file_path)
                            fuzzy_completer.searcher = searcher
                            logger.info(f"Loaded '{new_file_path}'. Found {len(searcher.unique_keys)} keys/columns.")
                        else:
                            logger.error(f"File not found at '{new_file_path}'")
                    else:
                        logger.info("Usage: /load <file_path>")
                elif query.startswith('/only'):
                    parts = query.split(maxsplit=1)
                    if len(parts) > 1:
                        filter_keys_str = parts[1].strip()
                        if filter_keys_str:
                            searcher._allowed_keys_filter = [k.strip() for k in filter_keys_str.split(',')]
                            searcher._apply_key_filter()
                            typer.echo(f"Fuzzy search now limited to keys containing: {filter_keys_str}")
                        else:
                            searcher._allowed_keys_filter = []
                            searcher._apply_key_filter()
                            typer.echo("Fuzzy search filter cleared. Searching all keys.")
                    else:
                        searcher._allowed_keys_filter = []
                        searcher._apply_key_filter()
                        typer.echo("Usage: /only <key1,key2,...> or /only to clear filter.")
                else:
                    typer.echo(f"Unknown command: {query}")
            else:
                matches = searcher.fuzzy_search(query, search_type=current_completion_type)
                searcher.display_matches(matches, search_type=current_completion_type)
        except (KeyboardInterrupt, EOFError):
            typer.echo("\nExiting.")
            break

if __name__ == "__main__":
    app()

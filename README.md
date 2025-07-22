# fuzzygrep

**fuzzygrep** is an interactive command-line tool for fuzzy searching, exploring, and inspecting JSON and CSV files.

---

## Features

- Fuzzy search for **keys** or **values** in deeply nested JSON and CSV files
- Visualize data with charts and graphs

---

## Installation

```bash
git clone https://github.com/yourname/fuzzygrep.git
cd fuzzygrep
pip install -r requirements.txt
```

## Usage

The basic way to use the program

```bash
python fuzzygrep.py data.json
```

## Options

```bash
python fuzzygrep.py <file> [--chart] [--histogram] [--verbose]
```

- ```--chart```		Show the rich tree view and exit
- ```--histogram```		Show frequency bar chart for keys / values
- ```--verbose```		Show detailed logs at the end of the program

## TODO

- Regex-based search
- Support .ndjson, .yaml
- GUI mode (optional)

## Contributing

Feel free to fork the repository and pull requests

---

Created with love by Anggi Ananda\
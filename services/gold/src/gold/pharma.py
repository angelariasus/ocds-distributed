"""Medicines: which purchased items are drugs, and whether they are in EsSalud's Petitorio.

An item is a medicine when its CUBSO classification is in segment 51 ("drugs and pharmaceutical
products", inherited from UNSPSC). For those, the free-text description is matched against the
Petitorio Farmacológico (EsSalud's formulary, Resolución N° 063-2026):

- ``dci+concentracion``: every word of the active ingredient (DCI) appears in the description and
  at least one strength (e.g. 500 MG) matches the Petitorio entry's specification;
- ``dci``: the active ingredient matches but no strength could be confirmed (other presentation,
  or the description omits it);
- ``fuzzy``: no exact ingredient match, but a close one (typos such as "PARACETAML");
- ``dci-parcial``: the description is a combination ("A + B") but only a single-ingredient
  entry matched, so the product itself is not in the Petitorio;
- ``none``: not found.

Every match keeps its method and score so its quality can be audited rather than assumed.
"""

import re
import unicodedata
from dataclasses import dataclass
from importlib import resources
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz, process

MEDICINE_SEGMENT = "51"
_STRENGTH = re.compile(r"(\d+(?:[.,]\d+)?)\s*(MCG|MG|G|UI|ML|%|MEQ|MMOL)(?![A-Z])")
_PAREN = re.compile(r"\(.*?\)")
_ALTERNATIVES = re.compile(r"\s+(?:ó|o|Ó|O)\s+")
_STOPWORDS = {"DE", "LA", "EL", "Y", "CON", "SIN", "COMO", "PARA", "EN", "X", "AGUA", "BASE", "SAL"}
# Salt and ester forms: the Petitorio names them, purchase descriptions usually do not.
_SALTS = {
    "CLORHIDRATO",
    "DICLORHIDRATO",
    "SULFATO",
    "MALEATO",
    "GLUCONATO",
    "SODICA",
    "SODICO",
    "DISODICO",
    "POTASICA",
    "POTASICO",
    "CALCICA",
    "CALCICO",
    "FOSFATO",
    "ACETATO",
    "BESILATO",
    "ESTEARATO",
    "ETILSUCCINATO",
    "PALMITATO",
    "CITRATO",
    "TARTRATO",
    "MESILATO",
    "HIDROBROMURO",
    "SUCCINATO",
    "MONOHIDRATO",
    "TRIHIDRATO",
    "ANHIDRO",
    "ANHIDRA",
    "FUMARATO",
    "BITARTRATO",
    "LACTATO",
    "DIPROPIONATO",
    "PROPIONATO",
    "VALERATO",
    "SODIO",
    "POTASIO",
    "MAGNESIO",
    "HIDROCLORURO",
    "BROMHIDRATO",
    "DIHIDRATO",
    "DISOPROXILO",
    "HEMIHIDRATO",
    "NITRATO",
    "OXALATO",
}


def normalize(text: str) -> str:
    text = unicodedata.normalize("NFKD", str(text or "")).encode("ascii", "ignore").decode()
    text = re.sub(r"[^A-Z0-9%.,+ ]", " ", text.upper())
    # Unglue "LATANOPROST0.005" and "100MGTB"-style text before tokenising.
    text = re.sub(r"(?<=[A-Z])(?=\d)", " ", text)
    text = re.sub(r"(?<=\d)(?=[A-Z]{2,})", " ", text)
    text = re.sub(r"\b(MCG|MG|ML|G|UI)(TB|TAB|CP|INY|AM|FR|SOB|SUS|SOL|CRM|GTS|CAP)\b", r"\1 \2", text)
    return re.sub(r"\s+", " ", text).strip()


def strengths(text: str) -> set[str]:
    out = set()
    for number, unit in _STRENGTH.findall(normalize(text)):
        value = number.replace(",", ".").rstrip("0").rstrip(".") if "." in number or "," in number else number
        out.add(f"{value}{unit}")
    return out


def dci_tokens(dci: str) -> frozenset[str]:
    """Core words of one ingredient name: parenthesised salt forms, salts and filler words removed."""
    base = normalize(_PAREN.sub(" ", str(dci or "")))
    words = [t.strip(".,") for t in re.split(r"[ +]", base)]
    words = [t for t in words if len(t) > 2 and t not in _STOPWORDS]
    core = [t for t in words if t not in _SALTS]
    return frozenset(core or words)


def dci_alternatives(dci: str) -> list[frozenset[str]]:
    """Every name a Petitorio row answers to.

    'DEXTROSA ó GLUCOSA EN AGUA' names one product two ways, and a parenthesis holds either a salt
    form ('(como besilato)', dropped) or a synonym ('DIMETICONA ACTIVADA (SIMETICONA)', kept).
    """
    names = _ALTERNATIVES.split(str(dci or ""))
    for inner in _PAREN.findall(str(dci or "")):
        words = set(re.split(r"[ +]", normalize(inner.strip("()"))))
        if words and not words & ({"COMO"} | _SALTS):
            names.append(inner.strip("()"))
    out = []
    for tokens in (dci_tokens(n) for n in names):
        if tokens and tokens not in out:
            out.append(tokens)
    return out


@dataclass(frozen=True)
class Entry:
    codigo: str
    dci: str
    especificacion: str
    tokens: frozenset[str]
    strengths: frozenset[str]


@dataclass(frozen=True)
class Match:
    codigo: str | None
    dci: str | None
    method: str
    score: float


def load_petitorio(path: Path | None = None) -> list[Entry]:
    path = path or resources.files("gold") / "reference" / "petitorio-essalud-2026.xls"
    book = pd.ExcelFile(path)
    main = pd.read_excel(book, "PETITORIO ESSALUD 2026", header=2, dtype=str)
    extra = pd.read_excel(book, "Hoja1", header=0, dtype=str).rename(columns={"CODIGO ": "CODIGO SAP"})
    frame = pd.concat([main, extra], ignore_index=True)
    entries = []
    for _, row in frame.iterrows():
        dci, codigo = row.get("DENOMINACION SEGÚN DCI"), row.get("CODIGO SAP")
        if not isinstance(dci, str) or not isinstance(codigo, str) or not re.fullmatch(r"\d{6,}", codigo.strip()):
            continue  # notes and blank rows in the spreadsheet
        spec = row.get("ESPECIFICACIONES TECNICAS") if isinstance(row.get("ESPECIFICACIONES TECNICAS"), str) else ""
        for tokens in dci_alternatives(dci):
            entries.append(Entry(codigo.strip(), dci.strip(), spec.strip(), tokens, frozenset(strengths(spec))))
    return entries


class Matcher:
    def __init__(self, entries: list[Entry]):
        self.entries = entries
        self.by_token: dict[str, list[Entry]] = {}
        for e in entries:
            for t in e.tokens:
                self.by_token.setdefault(t, []).append(e)
        self.vocabulary = sorted(self.by_token)
        self._cache: dict[str, Match] = {}

    def _find(self, words: set[str]) -> dict[int, Entry]:
        return {id(e): e for t in words for e in self.by_token.get(t, []) if e.tokens <= words}

    def match(self, description: str) -> Match:
        desc = normalize(description)
        if desc in self._cache:
            return self._cache[desc]
        words = {w.strip(".,") for w in re.split(r"[ +]", desc)}
        found, method, score = self._find(words), "dci", 90.0
        if not found:
            # Typos: correct each long word to the closest ingredient word, then retry.
            corrected, worst = set(words), 100.0
            for w in words:
                if len(w) >= 6 and w not in self.by_token:
                    hit = process.extractOne(w, self.vocabulary, scorer=fuzz.ratio, score_cutoff=88)
                    if hit:
                        corrected.add(hit[0])
                        worst = min(worst, float(hit[1]))
            found, method, score = self._find(corrected), "fuzzy", worst
        result = Match(None, None, "none", 0.0)
        if found:
            want = strengths(desc)
            # Prefer the most specific ingredient (combinations over single drugs), then strength overlap.
            best = max(found.values(), key=lambda e: (len(e.tokens), len(e.strengths & want)))
            if "+" in desc and len(best.tokens) == 1:
                method, score = "dci-parcial", 60.0
            elif method == "dci" and want and best.strengths & want:
                method, score = "dci+concentracion", 100.0
            result = Match(best.codigo, best.dci, method, score)
        self._cache[desc] = result
        return result


def is_medicine(clasificacion: str) -> bool:
    return str(clasificacion or "").startswith(MEDICINE_SEGMENT)

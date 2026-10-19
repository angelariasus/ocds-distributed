import pytest

from gold.pharma import Matcher, is_medicine, load_petitorio, normalize, strengths


@pytest.fixture(scope="module")
def matcher():
    return Matcher(load_petitorio())


def test_petitorio_loads_every_drug_row():
    entries = load_petitorio()
    assert 900 < len(entries) < 1100
    assert all(e.codigo.isdigit() for e in entries)


def test_normalize_and_strengths():
    assert normalize("Ciclosporina 100 mg / mL – solución") == "CICLOSPORINA 100 MG ML SOLUCION"
    assert strengths("EPOETINA ALFA 2000 UI/ML X 1 ML") == {"2000UI", "1ML"}
    assert strengths("Paracetamol 500mg tab") == {"500MG"}
    assert strengths("solución 0,5 % gotas") == {"0.5%"}


@pytest.mark.parametrize(
    "description,method,dci_word",
    [
        ("ACETAZOLAMIDA 250 MG TAB", "dci+concentracion", "ACETAZOLAMIDA"),
        ("ABACAVIR + LAMIVUDINA 120 MG + 60 MG", "dci+concentracion", "LAMIVUDINA"),
        ("ACETAZOLAMIDA 125 MG", "dci", "ACETAZOLAMIDA"),
        ("ACETAZOLAMIDS 250 MG TABLETA", "fuzzy", "ACETAZOLAMIDA"),
        # Real EsSalud descriptions the first version missed (salt forms, alternatives, glued text):
        ("AMLODIPINO 10 MG TAB", "dci+concentracion", "AMLODIPINO"),
        ("CEFTRIAXONA 1 G INY", "dci+concentracion", "CEFTRIAXONA"),
        ("CLORFENAMINA MALEATO10 MG/ML X 1 ML AM", "dci+concentracion", "CLORFENAMINA"),
        ("CILASTATINA + IMIPENEM 500 MG + 500 MG INY", "dci+concentracion", "IMIPENEM"),
        ("DOXORUBICINA 50 MG INY", "dci+concentracion", "DOXORUBICINA"),
        ("ESPIRONOLACTONA100 MGTB", "dci+concentracion", "ESPIRONOLACTONA"),
        ("SERTRALINA 50 MG TAB", "dci+concentracion", "SERTRALINA"),
        ("SIMETICONA 80 MG/ML SUS 15 ML", "dci+concentracion", "SIMETICONA"),
    ],
)
def test_matches_with_auditable_method(matcher, description, method, dci_word):
    m = matcher.match(description)
    assert m.method == method, m
    assert dci_word in normalize(m.dci)


def test_combination_wins_over_single_ingredient(matcher):
    m = matcher.match("ABACAVIR + LAMIVUDINA 120MG + 60MG TAB")
    assert "LAMIVUDINA" in normalize(m.dci) and "ABACAVIR" in normalize(m.dci)


@pytest.mark.parametrize("description", ["OLAPARIB 150 MG TAB", "TENOFOVIR 300 MG TAB"])
def test_drug_outside_the_formulary_is_not_matched(matcher, description):
    # Bought by EsSalud, not in the 2026 Petitorio (tenofovir only appears there in combinations).
    assert matcher.match(description).method == "none"


def test_non_drug_text_is_not_matched(matcher):
    assert matcher.match("SERVICIO DE LIMPIEZA Y MANTENIMIENTO DE AMBIENTES").method == "none"


def test_medicine_segment():
    assert is_medicine("5111170700392236") and not is_medicine("4214150100001") and not is_medicine(None)


@pytest.mark.parametrize("description", ["DEXTROSA 50 % INY 1 L", "LATANOPROST0.005 X 2.5 ML GOTAS OFTALMICAS"])
def test_alternative_names_and_glued_text_are_found(matcher, description):
    assert matcher.match(description).method in {"dci", "dci+concentracion"}


def test_combination_matched_to_a_single_ingredient_is_partial(matcher):
    m = matcher.match("SACUBITRILO + VALSARTAN - 24MG + 26MG - TAB")
    assert m.method == "dci-parcial" and "VALSART" in normalize(m.dci)

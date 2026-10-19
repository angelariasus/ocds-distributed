import numpy as np
import pandas as pd

from mlserve import model


def synthetic(n=400, seed=1):
    rng = np.random.default_rng(seed)
    metodo = rng.choice(["direct", "open", "selective"], n)
    base = np.where(metodo == "open", 90, np.where(metodo == "selective", 45, 15))
    monto = rng.lognormal(10, 1.5, n)
    return pd.DataFrame(
        {
            "monto_referencial": monto,
            "metodo": metodo,
            "categoria": rng.choice(["goods", "services", "works"], n),
            "departamento": rng.choice(["LIMA", "PIURA", "CUSCO"], n),
            "es_essalud": rng.choice([True, False], n),
            "lead_time_dias": base + np.log(monto) * 2 + rng.normal(0, 5, n),
            "fecha_convocatoria": pd.Timestamp("2025-01-01") + pd.to_timedelta(rng.integers(0, 600, n), unit="D"),
        }
    )


def test_train_learns_the_method_effect_and_reports_mae():
    pipeline, info = model.train(synthetic())
    assert 0 < info.training_rows < 400 and info.mae_dias < 15
    direct = model.predict(pipeline, 50000, "direct", "goods", "LIMA", True)
    open_ = model.predict(pipeline, 50000, "open", "goods", "LIMA", True)
    assert open_ > direct + 30


def test_predict_handles_unknown_categories_and_never_goes_negative():
    pipeline, _ = model.train(synthetic())
    assert model.predict(pipeline, 1.0, "weird", "unseen", "", False) >= 0.0


def test_recent_calls_are_left_out_as_censored():
    frame = synthetic()
    _, info = model.train(frame, horizon=180)
    cutoff = frame["fecha_convocatoria"].max() - pd.Timedelta(days=180)
    eligible = frame[(frame["fecha_convocatoria"] <= cutoff) & frame["lead_time_dias"].between(0, 180)]
    assert info.training_rows == len(eligible) and info.cohort_until == cutoff.date().isoformat()

"""Capa 4 - Visualización: figuras PNG a partir de las tablas Gold exportadas.

No requiere Spark (lee los CSV de outputs/gold y las métricas JSON), de modo que puede
regenerarse en segundos. Paleta: categórica en orden fijo (azul, naranja, aqua, amarillo),
secuencial de un solo tono (azul) y divergente azul<->rojo con punto medio gris.
"""
from __future__ import annotations

import io
import zipfile
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap, LogNorm  # noqa: E402

from .config import Settings  # noqa: E402
from .schema import DAY_NAMES_ES  # noqa: E402
from .utils import get_logger, read_json  # noqa: E402

log = get_logger("visualize")

SURFACE = "#fcfcfb"
INK, INK2, MUTED, GRID = "#0b0b0b", "#52514e", "#8a8984", "#e6e5e0"
CAT = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]          # orden fijo, nunca ciclado
SEQ = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#2a78d6", "#1c5cab", "#104281", "#0d366b"]
SEQ_CMAP = LinearSegmentedColormap.from_list("seq_blue", SEQ)
DIV_CMAP = LinearSegmentedColormap.from_list("div", ["#1c5cab", "#86b6ef", "#f0efec", "#ef9a8f", "#c03232"])
BOROUGH_COLOR = {"Manhattan": CAT[0], "Brooklyn": CAT[1], "Queens": CAT[2], "Bronx": CAT[3]}

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "font.family": "DejaVu Sans", "font.size": 10, "axes.titlesize": 12, "axes.titleweight": "bold",
    "axes.titlelocation": "left", "axes.labelcolor": INK2, "axes.edgecolor": GRID, "text.color": INK,
    "xtick.color": INK2, "ytick.color": INK2, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.7,
    "axes.spines.top": False, "axes.spines.right": False, "axes.axisbelow": True,
    "legend.frameon": False, "lines.linewidth": 2,
})


def _thousands(x, _=None):
    if abs(x) >= 1e6:
        return f"{x / 1e6:.1f} M".replace(".", ",")
    if abs(x) >= 1e3:
        return f"{x / 1e3:.0f} k"
    return f"{x:.0f}"


def _save(fig, out: Path, name: str, source: str = "Fuente: NYC TLC Yellow Taxi 2025 · procesamiento propio (Spark)") -> str:
    fig.text(0.01, -0.01, source, fontsize=7.5, color=MUTED, ha="left", va="top")
    path = out / f"{name}.png"
    fig.savefig(path, dpi=170, bbox_inches="tight")
    plt.close(fig)
    return path.name


# ------------------------------------------------------------------------------------
def fig_calidad(s: Settings, out: Path) -> str:
    dq = read_json(s.outputs_dir / "quality" / "dq_report.json")
    df = pd.DataFrame(dq["rules"]).sort_values("violations")
    fig, ax = plt.subplots(figsize=(8.5, 4.2))
    ax.barh(df["description"], df["violations"], color=CAT[0], height=0.6)
    for y, v in enumerate(df["violations"]):
        ax.text(v, y, f"  {v:,}".replace(",", "."), va="center", fontsize=8.5, color=INK2)
    ax.xaxis.set_major_formatter(_thousands)
    ax.grid(axis="y", visible=False)
    ax.set_title(f"Violaciones por regla de calidad ({dq['total'] / 1e6:.1f} M registros evaluados)".replace(".", ","))
    ax.set_xlabel("Registros que incumplen la regla (un registro puede incumplir varias)")
    return _save(fig, out, "fig01_calidad_reglas")


def _days_per_type(s: Settings) -> dict[bool, int]:
    daily = pd.read_csv(s.outputs_dir / "gold" / "serie_diaria.csv", parse_dates=["pickup_date"])
    wk = daily["pickup_date"].dt.dayofweek >= 5
    return {True: int(wk.sum()), False: int((~wk).sum())}


def fig_demanda_horaria(s: Settings, out: Path) -> str:
    g = pd.read_csv(s.outputs_dir / "gold" / "demanda_hora_dia.csv")
    g["weekend"] = g["pickup_dow"].isin([1, 7])
    days = _days_per_type(s)
    h = g.groupby(["weekend", "pickup_hour"]).trips.sum().reset_index()
    h["per_day"] = h.apply(lambda r: r.trips / days[bool(r.weekend)], axis=1)
    fig, ax = plt.subplots(figsize=(8.5, 4))
    for wk, color, label in [(False, CAT[0], "Día laborable"), (True, CAT[1], "Fin de semana")]:
        d = h[h.weekend == wk]
        ax.plot(d.pickup_hour, d.per_day, color=color, label=label, marker="o", markersize=3.5)
        ax.annotate(label, (23, d.per_day.iloc[-1]), xytext=(6, 0), textcoords="offset points",
                    color=INK2, fontsize=9, va="center")
    ax.set_xticks(range(0, 24, 2))
    ax.set_xlim(0, 25.5)
    ax.yaxis.set_major_formatter(_thousands)
    ax.set_xlabel("Hora de recogida")
    ax.set_ylabel("Viajes promedio por hora")
    ax.set_title("Curva de demanda diaria: pico vespertino laborable y madrugada de fin de semana")
    ax.legend(loc="upper left")
    return _save(fig, out, "fig02_demanda_horaria")


def fig_heatmap(s: Settings, out: Path) -> str:
    g = pd.read_csv(s.outputs_dir / "gold" / "demanda_hora_dia.csv")
    order = [2, 3, 4, 5, 6, 7, 1]
    m = g.pivot(index="pickup_dow", columns="pickup_hour", values="trips").reindex(order)
    fig, ax = plt.subplots(figsize=(9, 3.4))
    im = ax.imshow(m.values, aspect="auto", cmap=SEQ_CMAP)
    ax.set_yticks(range(7), [DAY_NAMES_ES[d] for d in order])
    ax.set_xticks(range(0, 24, 2), range(0, 24, 2))
    ax.grid(False)
    ax.set_xlabel("Hora de recogida")
    cb = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.02, format=_thousands)
    cb.outline.set_visible(False)
    cb.set_label("Viajes en 2025", color=INK2)
    ax.set_title("Mapa de calor de la demanda por día y hora")
    return _save(fig, out, "fig03_heatmap_dia_hora")


def _read_zone_shapes(s: Settings):
    import shapefile  # pyshp

    with zipfile.ZipFile(s.zone_shapefile_zip) as z:
        names = z.namelist()
        get = lambda ext: io.BytesIO(z.read(next(n for n in names if n.lower().endswith(ext))))
        reader = shapefile.Reader(shp=get(".shp"), shx=get(".shx"), dbf=get(".dbf"))
        fields = [f[0] for f in reader.fields[1:]]
        idx = fields.index("LocationID")
        return [(int(rec[idx]), shp) for rec, shp in zip(reader.records(), reader.shapes())]


def fig_mapa(s: Settings, out: Path) -> str:
    from matplotlib.collections import PolyCollection

    g = pd.read_csv(s.outputs_dir / "gold" / "demanda_zona.csv").set_index("location_id")
    shapes = _read_zone_shapes(s)
    polys, vals = [], []
    for loc, shp in shapes:
        pts = np.array(shp.points)
        parts = list(shp.parts) + [len(pts)]
        v = g.pickups.get(loc, 0)
        for a, b in zip(parts[:-1], parts[1:]):
            polys.append(pts[a:b])
            vals.append(max(v, 1))
    fig, ax = plt.subplots(figsize=(7.5, 7.5))
    pc = PolyCollection(polys, array=np.array(vals), cmap=SEQ_CMAP, norm=LogNorm(vmin=1, vmax=max(vals)),
                        edgecolors=SURFACE, linewidths=0.25)
    ax.add_collection(pc)
    ax.autoscale_view()
    ax.set_aspect("equal")
    ax.axis("off")
    cb = fig.colorbar(pc, ax=ax, fraction=0.03, pad=0.01, format=_thousands)
    cb.outline.set_visible(False)
    cb.set_label("Recogidas en 2025 (escala logarítmica)", color=INK2)
    top = g.sort_values("pickups", ascending=False).head(3)
    ax.set_title("Demanda por zona TLC: concentración en Manhattan y aeropuertos")
    txt = "Top 3: " + "; ".join(f"{z} ({p / 1e6:.2f} M)".replace(".", ",") for z, p in zip(top.zone, top.pickups))
    ax.text(0.0, -0.02, txt, transform=ax.transAxes, fontsize=8.5, color=INK2)
    return _save(fig, out, "fig04_mapa_demanda_zonas")


def fig_top_zonas(s: Settings, out: Path) -> str:
    g = pd.read_csv(s.outputs_dir / "gold" / "demanda_zona.csv").nlargest(15, "pickups").iloc[::-1]
    fig, ax = plt.subplots(figsize=(8.5, 5))
    ax.barh(g.zone + " · " + g.borough, g.pickups, color=CAT[0], height=0.62)
    for y, (p, sh) in enumerate(zip(g.pickups, g.pickup_share_pct)):
        ax.text(p, y, f"  {sh:.1f} %".replace(".", ","), va="center", fontsize=8.5, color=INK2)
    ax.xaxis.set_major_formatter(_thousands)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("Recogidas en 2025 (etiqueta = % del total)")
    ax.set_title(f"Las 15 zonas con más recogidas concentran el {g.pickup_share_pct.sum():.0f} % de la demanda")
    return _save(fig, out, "fig05_top_zonas")


def fig_velocidad(s: Settings, out: Path) -> str:
    g = pd.read_csv(s.outputs_dir / "gold" / "velocidad_hora_borough.csv")
    g = g[~g.is_weekend]
    fig, ax = plt.subplots(figsize=(8.5, 4.2))
    for b, color in BOROUGH_COLOR.items():
        d = g[g.borough == b].sort_values("pickup_hour")
        if d.empty:
            continue
        ax.plot(d.pickup_hour, d.avg_speed_mph, color=color, label=b)
        ax.annotate(b, (23, d.avg_speed_mph.iloc[-1]), xytext=(6, 0), textcoords="offset points",
                    color=INK2, fontsize=9, va="center")
    ax.set_xticks(range(0, 24, 2))
    ax.set_xlim(0, 26)
    ax.set_xlabel("Hora de recogida (días laborables)")
    ax.set_ylabel("Velocidad media del viaje (mph)")
    ax.set_title("Curvas de tráfico: la velocidad cae a su mínimo entre 8 h y 18 h")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=4)
    return _save(fig, out, "fig06_velocidad_hora_borough")


def fig_mensual(s: Settings, out: Path) -> str:
    g = pd.read_csv(s.outputs_dir / "gold" / "mensual_borough.csv")
    m = g.groupby("month").agg(trips=("trips", "sum"), revenue=("revenue_usd", "sum")).reset_index()
    m["ticket"] = m.revenue / m.trips
    meses = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"]
    labels = [meses[i - 1] for i in m.month]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.8))
    a1.bar(labels, m.trips, color=CAT[0], width=0.65)
    a1.yaxis.set_major_formatter(_thousands)
    a1.grid(axis="x", visible=False)
    a1.set_title("Viajes válidos por mes")
    a2.plot(labels, m.ticket, color=CAT[0], marker="o", markersize=4)
    a2.set_title("Ingreso medio por viaje (USD)")
    a2.set_ylim(m.ticket.min() * 0.95, m.ticket.max() * 1.03)
    for a in (a1, a2):
        a.tick_params(axis="x", labelsize=8.5)
    fig.suptitle("Estacionalidad mensual 2025", x=0.01, ha="left", fontweight="bold")
    fig.tight_layout()
    return _save(fig, out, "fig07_mensual")


def fig_serie_diaria(s: Settings, out: Path) -> str:
    d = pd.read_csv(s.outputs_dir / "gold" / "serie_diaria.csv", parse_dates=["pickup_date"])
    d["ma7"] = d.trips.rolling(7, center=True).mean()
    fig, ax = plt.subplots(figsize=(10, 3.6))
    ax.plot(d.pickup_date, d.trips, color="#9ec5f4", linewidth=1, label="Diario")
    ax.plot(d.pickup_date, d.ma7, color=CAT[0], linewidth=2, label="Media móvil 7 días")
    lo = d.nsmallest(3, "trips")
    ax.scatter(lo.pickup_date, lo.trips, s=30, color=CAT[1], zorder=3, edgecolor=SURFACE, linewidth=1.5)
    for dt, t in zip(lo.pickup_date, lo.trips):
        ax.annotate(dt.strftime("%d-%b"), (dt, t), xytext=(4, -10), textcoords="offset points",
                    fontsize=8, color=INK2)
    ax.yaxis.set_major_formatter(_thousands)
    ax.set_ylabel("Viajes por día")
    ax.legend(loc="upper left", ncol=2)
    ax.set_title("Serie diaria 2025 (mínimos marcados: festivos y eventos climáticos)")
    return _save(fig, out, "fig08_serie_diaria")


def fig_cbd(s: Settings, out: Path) -> str:
    g = pd.read_csv(s.outputs_dir / "gold" / "cargo_congestion_cbd.csv")
    g = g[~g.is_weekend]
    by_h = g.groupby("pickup_hour").apply(lambda d: pd.Series({
        "pct": np.average(d.pct_with_cbd_fee, weights=d.trips),
        "with": np.average(d.speed_with_fee_mph.fillna(0), weights=d.trips),
        "without": np.average(d.speed_without_fee_mph.fillna(0), weights=d.trips)}), include_groups=False).reset_index()
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.8))
    a1.bar(by_h.pickup_hour, by_h.pct, color=CAT[0], width=0.7)
    a1.set_ylim(0, 100)
    a1.set_xlabel("Hora (días laborables)")
    a1.set_title("% de viajes que pagan el cargo CBD")
    a1.grid(axis="x", visible=False)
    for col, color, label in [("with", CAT[0], "Con cargo CBD"), ("without", CAT[1], "Sin cargo CBD")]:
        a2.plot(by_h.pickup_hour, by_h[col], color=color, label=label)
    a2.set_xlabel("Hora (días laborables)")
    a2.set_title("Velocidad media (mph)")
    a2.legend(loc="upper center", bbox_to_anchor=(0.5, -0.2), ncol=2)
    fig.suptitle("Peaje de congestión de Manhattan (CBD) en su primer año", x=0.01, ha="left", fontweight="bold")
    fig.tight_layout()
    return _save(fig, out, "fig09_cargo_cbd")


def fig_modelo(s: Settings, out: Path) -> list[str]:
    metrics = read_json(s.outputs_dir / "metrics" / "model_metrics.json")
    names = []
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.3))
    for ax, (target, res) in zip(axes, metrics["targets"].items()):
        p = pd.read_csv(s.outputs_dir / "model" / f"predicciones_muestra_{target}.csv")
        lim = np.percentile(p[target], 99)
        hb = ax.hexbin(p[target], p.prediction, gridsize=45, cmap=SEQ_CMAP, mincnt=1,
                       extent=(0, lim, 0, lim), bins="log", linewidths=0.2, edgecolors=SURFACE)
        ax.plot([0, lim], [0, lim], color=CAT[1], linewidth=1.5, linestyle="--", label="Predicción perfecta")
        m = res["models"]["gbt"]
        ax.set_title(f"{res['label']}\nGBT: R² = {m['r2']:.3f} · MAE = {m['mae']:.2f}".replace(".", ","), fontsize=10.5)
        ax.set_xlabel("Valor real")
        ax.set_ylabel("Valor predicho")
        ax.legend(loc="upper left")
        ax.grid(False)
    fig.suptitle("Modelo MLlib (GBT) sobre el conjunto de prueba", x=0.01, ha="left", fontweight="bold")
    fig.tight_layout()
    names.append(_save(fig, out, "fig10_modelo_real_vs_pred"))

    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for ax, (target, res) in zip(axes, metrics["targets"].items()):
        fi = pd.DataFrame(res["feature_importance"]).head(8).iloc[::-1]
        ax.barh(fi.feature, fi.importance, color=CAT[0], height=0.6)
        ax.set_title(res["label"], fontsize=10.5)
        ax.grid(axis="y", visible=False)
        ax.set_xlabel("Importancia relativa (GBT)")
    fig.suptitle("¿Qué variables explican la duración y el costo?", x=0.01, ha="left", fontweight="bold")
    fig.tight_layout()
    names.append(_save(fig, out, "fig11_importancia_variables"))
    return names


def fig_pagos(s: Settings, out: Path) -> str:
    g = pd.read_csv(s.outputs_dir / "gold" / "pagos_propinas.csv").sort_values("trips")
    fig, ax = plt.subplots(figsize=(8, 3.4))
    ax.barh(g.payment_label, g.share_pct, color=CAT[0], height=0.6)
    for y, (sh, tip) in enumerate(zip(g.share_pct, g.avg_tip_pct)):
        tip_txt = f" · propina media {tip:.1f} %" if pd.notna(tip) and tip > 0.5 else ""
        ax.text(sh, y, f"  {sh:.1f} %{tip_txt}".replace(".", ","), va="center", fontsize=8.5, color=INK2)
    ax.set_xlim(0, g.share_pct.max() * 1.45)
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("% de viajes válidos")
    ax.set_title("Formas de pago y propina media")
    return _save(fig, out, "fig12_pagos_propinas")


def fig_correlaciones(s: Settings, out: Path) -> str:
    c = pd.read_csv(s.outputs_dir / "gold" / "correlaciones.csv")
    vars_ = list(dict.fromkeys(list(c.var_a) + list(c.var_b)))
    m = pd.DataFrame(np.eye(len(vars_)), index=vars_, columns=vars_)
    for a, b, v in c[["var_a", "var_b", "pearson"]].itertuples(index=False):
        m.loc[a, b] = m.loc[b, a] = v
    fig, ax = plt.subplots(figsize=(7.5, 6.2))
    im = ax.imshow(m.values, cmap=DIV_CMAP, vmin=-1, vmax=1)
    ax.set_xticks(range(len(vars_)), vars_, rotation=45, ha="right", fontsize=8.5)
    ax.set_yticks(range(len(vars_)), vars_, fontsize=8.5)
    for i in range(len(vars_)):
        for j in range(len(vars_)):
            if i != j and abs(m.values[i, j]) >= 0.3:
                ax.text(j, i, f"{m.values[i, j]:.2f}".replace(".", ","), ha="center", va="center", fontsize=7,
                        color="white" if abs(m.values[i, j]) > 0.7 else INK)
    ax.grid(False)
    cb = fig.colorbar(im, ax=ax, fraction=0.04, pad=0.02)
    cb.outline.set_visible(False)
    ax.set_title("Correlación de Pearson (|r| ≥ 0,3 rotulado)")
    return _save(fig, out, "fig13_correlaciones")


def run(settings: Settings) -> list[str]:
    out = settings.outputs_dir / "figures"
    out.mkdir(parents=True, exist_ok=True)
    produced: list[str] = []
    for fn in (fig_calidad, fig_demanda_horaria, fig_heatmap, fig_mapa, fig_top_zonas, fig_velocidad,
               fig_mensual, fig_serie_diaria, fig_cbd, fig_modelo, fig_pagos, fig_correlaciones):
        try:
            res = fn(settings, out)
            produced.extend(res if isinstance(res, list) else [res])
        except FileNotFoundError as exc:
            log.warning("Figura %s omitida: falta insumo (%s)", fn.__name__, exc)
        except Exception:
            log.exception("Error generando %s", fn.__name__)
    log.info("%d figuras generadas en %s", len(produced), out)
    return produced

import io
import math
import re
import time
import zipfile
from typing import Optional, Tuple, List, Dict

import pandas as pd
import requests
import streamlit as st
import folium
from streamlit_folium import st_folium
from tenacity import retry, stop_after_attempt, wait_exponential

# =========================
# Config
# =========================
st.set_page_config(page_title="Carte → entreprises proches → bilans ZIP", layout="wide")

SEARCH_API_BASE = "https://recherche-entreprises.api.gouv.fr"
API_ENT_BASE = "https://entreprise.api.gouv.fr"
ADRESSE_BASE = "https://api-adresse.data.gouv.fr"  # BAN (search + reverse)

TOKEN = st.secrets.get("API_ENTREPRISE_TOKEN", "")
DEFAULT_CONTEXT = st.secrets.get("DEFAULT_CONTEXT", "comptes-annuels")
DEFAULT_RECIPIENT = st.secrets.get("DEFAULT_RECIPIENT", "")
DEFAULT_OBJECT = st.secrets.get("DEFAULT_OBJECT", "telechargement-comptes-annuels")

# =========================
# Utils
# =========================
def only_digits(s: str) -> str:
    return re.sub(r"\D", "", (s or "").strip())

def _short(text: str, n=800) -> str:
    text = text or ""
    return text[:n] + ("…" if len(text) > n else "")

def normalize_naf(code: str) -> str:
    code = (code or "").strip().upper()
    if not code:
        return ""
    if re.fullmatch(r"\d{2}\.\d{2}[A-Z]", code):
        return code
    if re.fullmatch(r"\d{4}[A-Z]", code):
        return f"{code[:2]}.{code[2:4]}{code[4]}"
    return code

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

# =========================
# HTTP (retry + erreurs lisibles)
# =========================
@retry(stop=stop_after_attempt(4), wait=wait_exponential(min=1, max=10), reraise=True)
def get_json(url: str, headers=None, params=None, timeout=35) -> dict:
    try:
        r = requests.get(url, headers=headers, params=params, timeout=timeout)
    except requests.RequestException as e:
        raise RuntimeError(f"Network error calling {url}: {e}") from e

    ctype = (r.headers.get("content-type") or "").lower()
    payload = None
    if "application/json" in ctype:
        try:
            payload = r.json()
        except Exception:
            payload = None

    if r.status_code in (429, 500, 502, 503, 504):
        msg = payload if payload is not None else _short(r.text)
        raise RuntimeError(f"Transient HTTP {r.status_code} on {url} params={params} body={msg}")

    if r.status_code >= 400:
        msg = payload if payload is not None else _short(r.text)
        raise RuntimeError(f"HTTP {r.status_code} on {url} params={params} body={msg}")

    return payload if payload is not None else r.json()

@retry(stop=stop_after_attempt(4), wait=wait_exponential(min=1, max=10), reraise=True)
def download_bytes(url: str, headers=None, timeout=70) -> bytes:
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
    except requests.RequestException as e:
        raise RuntimeError(f"Download network error: {e}") from e

    if r.status_code in (429, 500, 502, 503, 504):
        raise RuntimeError(f"Transient download HTTP {r.status_code} for {url}")

    if r.status_code >= 400:
        raise RuntimeError(f"Download HTTP {r.status_code} for {url} body={_short(r.text)}")

    return r.content

# =========================
# APIs (cache)
# =========================
@st.cache_data(ttl=24 * 3600, show_spinner=False)
def reverse_postcode(lat: float, lon: float) -> Optional[str]:
    data = get_json(f"{ADRESSE_BASE}/reverse/", params={"lat": lat, "lon": lon}, timeout=20)
    feats = data.get("features") or []
    if not feats:
        return None
    props = feats[0].get("properties") or {}
    return props.get("postcode")

@st.cache_data(ttl=7 * 24 * 3600, show_spinner=False)
def geocode_addr(addr: str) -> Tuple[Optional[float], Optional[float]]:
    data = get_json(f"{ADRESSE_BASE}/search/", params={"q": addr, "limit": 1}, timeout=20)
    feats = data.get("features") or []
    if not feats:
        return None, None
    lon, lat = feats[0]["geometry"]["coordinates"]
    return float(lat), float(lon)

@st.cache_data(ttl=20 * 60, show_spinner=False)
def search_companies_by_cp(code_postal: str, code_naf: str, per_page: int = 25, page: int = 1) -> dict:
    # ✅ API impose 1..25
    per_page = max(1, min(int(per_page), 25))
    params = {"code_postal": code_postal, "page": page, "per_page": per_page}
    if code_naf:
        params["code_naf"] = code_naf
    return get_json(f"{SEARCH_API_BASE}/search", params=params, timeout=35)

@st.cache_data(ttl=60 * 60, show_spinner=False)
def actes_bilans(siren: str) -> dict:
    url = f"{API_ENT_BASE}/v3/inpi/rne/unites_legales/open_data/{siren}/actes_bilans"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    params = {"context": DEFAULT_CONTEXT, "recipient": DEFAULT_RECIPIENT, "object": DEFAULT_OBJECT}
    return get_json(url, headers=headers, params=params, timeout=35)

# =========================
# ZIP builder
# =========================
def pick_pdf_urls(data: dict) -> List[Tuple[str, str]]:
    out = []
    bilans = data.get("bilans") or []
    for b in bilans:
        url = b.get("url") or b.get("url_document") or b.get("url_bilan")
        if not url:
            continue
        date_key = b.get("date_cloture") or b.get("date_depot") or b.get("date") or "date_inconnue"
        out.append((f"bilan_{date_key}.pdf", url))
    return out

def build_zip(selected: List[Dict]) -> bytes:
    buf = io.BytesIO()
    headers = {"Authorization": f"Bearer {TOKEN}"}

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for ent in selected:
            siren = ent["siren"]
            name = (ent.get("denomination") or "entreprise").replace("/", "-").replace("\\", "-")[:80]
            folder = f"{siren}_{name}"

            try:
                data = actes_bilans(siren)
            except Exception as e:
                zf.writestr(f"{folder}/README_erreur.txt", f"Erreur actes_bilans: {e}\n")
                continue

            pdfs = pick_pdf_urls(data)
            if not pdfs:
                zf.writestr(f"{folder}/README.txt", "Aucun bilan public trouvé via actes_bilans.\n")
                continue

            for filename, url in pdfs:
                time.sleep(0.3)
                try:
                    content = download_bytes(url, headers=headers)
                    zf.writestr(f"{folder}/{filename}", content)
                except Exception as e:
                    zf.writestr(f"{folder}/ERREUR_{filename}.txt", f"Erreur download: {e}\nURL: {url}\n")

    buf.seek(0)
    return buf.read()

# =========================
# Session state
# =========================
st.session_state.setdefault("click_latlon", None)
st.session_state.setdefault("results_df", None)
st.session_state.setdefault("selected_sirens", [])
st.session_state.setdefault("last_cp", None)

# =========================
# UI
# =========================
st.title("Carte → entreprises les plus proches → téléchargement bilans (ZIP)")

if not TOKEN:
    st.error("Secret manquant : API_ENTREPRISE_TOKEN")
if not DEFAULT_RECIPIENT:
    st.warning("Secret manquant : DEFAULT_RECIPIENT (SIRET organisme). Sans ça, l’API Entreprise peut refuser.")

left, right = st.columns([1.25, 1])

with left:
    st.subheader("1) Clique sur la carte pour choisir un point")

    naf_in = st.text_input("Filtre NAF (optionnel) — ex: 56.10A", value="", key="naf_input")
    naf = normalize_naf(naf_in)

    candidates_per_page = st.slider("Taille du pool candidat (max 25 imposé par l’API)", 10, 25, 25, 5)
    use_two_pages = st.checkbox("Prendre 2 pages (jusqu’à 50 candidats) pour mieux trouver les plus proches", value=True)

    default_center = st.session_state["click_latlon"] or (48.5, -2.8)
    m = folium.Map(location=default_center, zoom_start=10, control_scale=True)

    if st.session_state["click_latlon"]:
        folium.Marker(st.session_state["click_latlon"], tooltip="Point sélectionné",
                      icon=folium.Icon(color="red")).add_to(m)

    map_state = st_folium(m, height=520, width=None)

    if map_state and map_state.get("last_clicked"):
        lat = map_state["last_clicked"]["lat"]
        lon = map_state["last_clicked"]["lng"]
        st.session_state["click_latlon"] = (lat, lon)

    col_a, col_b = st.columns([1, 1])

    with col_a:
        if st.button("2) Trouver les 10 entreprises les plus proches", type="primary"):
            if not st.session_state["click_latlon"]:
                st.warning("Clique d’abord sur la carte.")
            else:
                lat, lon = st.session_state["click_latlon"]

                try:
                    cp = reverse_postcode(lat, lon)
                except Exception as e:
                    st.error("Erreur reverse géocodage (code postal)")
                    st.exception(e)
                    st.stop()

                if not cp:
                    st.error("Impossible de déterminer un code postal à cet endroit.")
                    st.stop()

                st.session_state["last_cp"] = cp

                # ✅ on respecte per_page <= 25 ; option 2 pages
                try:
                    with st.spinner(f"Recherche entreprises (CP {cp})…"):
                        res1 = search_companies_by_cp(code_postal=cp, code_naf=naf, per_page=candidates_per_page, page=1)
                        results = (res1.get("results") or res1.get("entreprises") or [])
                        if use_two_pages:
                            res2 = search_companies_by_cp(code_postal=cp, code_naf=naf, per_page=candidates_per_page, page=2)
                            results += (res2.get("results") or res2.get("entreprises") or [])
                except Exception as e:
                    st.error("Erreur sur recherche-entreprises.api.gouv.fr")
                    st.write({"code_postal": cp, "code_naf": naf, "per_page": candidates_per_page, "pages": 2 if use_two_pages else 1})
                    st.exception(e)
                    st.stop()

                if not results:
                    st.info("Aucun résultat. Essaie sans NAF ou clique ailleurs.")
                    st.session_state["results_df"] = pd.DataFrame()
                else:
                    rows = []
                    for r in results:
                        siren = only_digits(r.get("siren") or "")
                        if len(siren) != 9:
                            continue
                        denom = r.get("denomination") or r.get("nom_complet") or r.get("nom") or ""
                        adresse = r.get("adresse") or r.get("adresse_complete") or ""
                        ville = r.get("ville") or r.get("commune") or ""
                        full_addr = adresse or f"{denom} {cp} {ville}"
                        rows.append({"siren": siren, "denomination": denom, "adresse": adresse, "ville": ville, "full_addr": full_addr})

                    df = pd.DataFrame(rows).drop_duplicates(subset=["siren"])
                    if df.empty:
                        st.info("Résultats vides après nettoyage.")
                        st.session_state["results_df"] = df
                    else:
                        with st.spinner("Géocodage + tri par distance…"):
                            lats, lons, dists = [], [], []
                            for addr in df["full_addr"].tolist():
                                la, lo = geocode_addr(addr)
                                lats.append(la); lons.append(lo)
                                if la is None or lo is None:
                                    dists.append(10**9)
                                else:
                                    dists.append(haversine_km(lat, lon, la, lo))
                            df["lat"] = lats
                            df["lon"] = lons
                            df["distance_km"] = dists

                        df = df.sort_values("distance_km").head(10).reset_index(drop=True)
                        st.session_state["results_df"] = df

                        allowed = set(df["siren"].tolist())
                        st.session_state["selected_sirens"] = [s for s in st.session_state["selected_sirens"] if s in allowed]

    with col_b:
        if st.button("Réinitialiser"):
            st.session_state["click_latlon"] = None
            st.session_state["results_df"] = None
            st.session_state["selected_sirens"] = []
            st.session_state["last_cp"] = None
            st.rerun()

    if st.session_state["last_cp"]:
        st.caption(f"Code postal détecté : **{st.session_state['last_cp']}** | NAF: **{naf or '—'}**")

with right:
    st.subheader("3) Sélection (max 5) puis ZIP bilans")

    df = st.session_state.get("results_df")
    if df is None:
        st.info("Clique sur la carte, puis lance la recherche.")
        st.stop()

    if df.empty:
        st.info("Pas de résultats à afficher.")
        st.stop()

    st.dataframe(df[["siren", "denomination", "adresse", "ville", "distance_km"]], use_container_width=True)

    options = df["siren"].tolist()
    default_sel = [s for s in st.session_state["selected_sirens"] if s in options]

    selected = st.multiselect(
        "Entreprises sélectionnées",
        options=options,
        default=default_sel,
        max_selections=5,
        key="selected_multiselect",
    )
    st.session_state["selected_sirens"] = selected

    selected_rows = df[df["siren"].isin(selected)][["siren", "denomination"]].to_dict("records")

    disabled = (len(selected_rows) == 0) or (not TOKEN) or (not DEFAULT_RECIPIENT)
    if st.button("4) Télécharger les bilans (ZIP)", disabled=disabled):
        with st.spinner("Téléchargement + création du ZIP…"):
            zip_bytes = build_zip(selected_rows)

        st.download_button(
            "⬇️ Télécharger le ZIP",
            data=zip_bytes,
            file_name="bilans_selection.zip",
            mime="application/zip",
        )

    if not TOKEN or not DEFAULT_RECIPIENT:
        st.warning("Secrets requis : API_ENTREPRISE_TOKEN + DEFAULT_RECIPIENT (SIRET).")

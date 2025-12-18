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

st.set_page_config(page_title="Carte → entreprises proches → bilans ZIP", layout="wide")

SEARCH_API_BASE = "https://recherche-entreprises.api.gouv.fr"
API_ENT_BASE = "https://entreprise.api.gouv.fr"
ADRESSE_BASE = "https://api-adresse.data.gouv.fr"  # via geo.api.gouv.fr

TOKEN = st.secrets.get("API_ENTREPRISE_TOKEN", "")
DEFAULT_CONTEXT = st.secrets.get("DEFAULT_CONTEXT", "comptes-annuels")
DEFAULT_RECIPIENT = st.secrets.get("DEFAULT_RECIPIENT", "")
DEFAULT_OBJECT = st.secrets.get("DEFAULT_OBJECT", "telechargement-comptes-annuels")

# -------------------- helpers --------------------
def only_digits(s: str) -> str:
    return re.sub(r"\D", "", (s or "").strip())

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    # distance "à vol d'oiseau"
    R = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

@retry(stop=stop_after_attempt(4), wait=wait_exponential(min=1, max=10))
def get_json(url: str, headers=None, params=None, timeout=30) -> dict:
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    if r.status_code in (429, 500, 502, 503, 504):
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:200]}")
    r.raise_for_status()
    return r.json()

@st.cache_data(ttl=3600, show_spinner=False)
def reverse_geocode_cp(lat: float, lon: float) -> Optional[str]:
    # Reverse BAN: récupère le code postal
    data = get_json(f"{ADRESSE_BASE}/reverse/", params={"lat": lat, "lon": lon})
    feats = data.get("features") or []
    if not feats:
        return None
    props = feats[0].get("properties") or {}
    return props.get("postcode")

@st.cache_data(ttl=3600, show_spinner=False)
def geocode_one(addr: str) -> Tuple[Optional[float], Optional[float]]:
    data = get_json(f"{ADRESSE_BASE}/search/", params={"q": addr, "limit": 1})
    feats = data.get("features") or []
    if not feats:
        return None, None
    lon, lat = feats[0]["geometry"]["coordinates"]
    return float(lat), float(lon)

@st.cache_data(ttl=1800, show_spinner=False)
def search_companies(code_postal: str, code_naf: str, per_page: int = 25) -> dict:
    # API Recherche d'entreprises
    params = {"code_postal": code_postal, "code_naf": code_naf or None, "page": 1, "per_page": per_page}
    params = {k: v for k, v in params.items() if v}
    return get_json(f"{SEARCH_API_BASE}/search", params=params)

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_actes_bilans(siren: str) -> dict:
    url = f"{API_ENT_BASE}/v3/inpi/rne/unites_legales/open_data/{siren}/actes_bilans"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    params = {"context": DEFAULT_CONTEXT, "recipient": DEFAULT_RECIPIENT, "object": DEFAULT_OBJECT}
    return get_json(url, headers=headers, params=params)

@retry(stop=stop_after_attempt(4), wait=wait_exponential(min=1, max=10))
def download_pdf(url: str) -> bytes:
    headers = {"Authorization": f"Bearer {TOKEN}"}
    r = requests.get(url, headers=headers, timeout=60)
    if r.status_code in (429, 500, 502, 503, 504):
        raise RuntimeError(f"HTTP {r.status_code}")
    r.raise_for_status()
    return r.content

def build_zip(selected: List[Dict]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for ent in selected:
            siren = ent["siren"]
            name = (ent.get("denomination") or "entreprise").replace("/", "-")[:80]
            data = fetch_actes_bilans(siren)

            bilans = data.get("bilans") or []
            if not bilans:
                zf.writestr(f"{siren}_{name}/README.txt", "Aucun bilan public trouvé via actes_bilans.\n")
                continue

            for b in bilans:
                # champs peuvent varier : on tente plusieurs clés "url"
                pdf_url = b.get("url") or b.get("url_document") or b.get("url_bilan")
                if not pdf_url:
                    continue
                date_key = b.get("date_cloture") or b.get("date_depot") or "date_inconnue"
                time.sleep(0.25)  # anti-rafale
                content = download_pdf(pdf_url)
                zf.writestr(f"{siren}_{name}/bilan_{date_key}.pdf", content)
    buf.seek(0)
    return buf.read()

# -------------------- session state --------------------
st.session_state.setdefault("click_latlon", None)      # (lat, lon)
st.session_state.setdefault("results_df", None)        # dataframe top 10
st.session_state.setdefault("selected_sirens", [])     # persistant

# -------------------- UI --------------------
st.title("Carte → entreprises les plus proches → bilans en ZIP")

left, right = st.columns([1.2, 1])

with left:
    st.subheader("1) Clique sur la carte pour choisir un point")
    naf = st.text_input("Code NAF (optionnel, ex: 56.10A)", key="naf_input")

    # Carte Folium
    start_location = st.session_state["click_latlon"] or (48.5, -2.8)  # Bretagne par défaut
    m = folium.Map(location=start_location, zoom_start=10, control_scale=True)

    if st.session_state["click_latlon"]:
        folium.Marker(
            location=st.session_state["click_latlon"],
            tooltip="Point sélectionné",
            icon=folium.Icon(color="red")
        ).add_to(m)

    map_state = st_folium(m, height=520, width=None)

    # Capture du clic
    if map_state and map_state.get("last_clicked"):
        lat = map_state["last_clicked"]["lat"]
        lon = map_state["last_clicked"]["lng"]
        st.session_state["click_latlon"] = (lat, lon)

    st.caption("Astuce : clique une fois, puis utilise le bouton de recherche en dessous (évite les reruns ‘surprise’).")

    if st.button("2) Trouver les 10 entreprises les plus proches", type="primary"):
        if not st.session_state["click_latlon"]:
            st.warning("Clique d’abord sur la carte.")
        else:
            lat, lon = st.session_state["click_latlon"]
            cp = reverse_geocode_cp(lat, lon)
            if not cp:
                st.error("Impossible de déterminer un code postal à cet endroit.")
            else:
                with st.spinner(f"Recherche via code postal {cp}…"):
                    res = search_companies(code_postal=cp, code_naf=naf, per_page=40)
                    results = res.get("results") or res.get("entreprises") or []
                    if not results:
                        st.info("Aucun résultat. Essaie sans NAF ou clique dans une zone plus dense.")
                    else:
                        rows = []
                        for r in results:
                            siren = only_digits(r.get("siren") or "")
                            denom = r.get("denomination") or r.get("nom_complet") or r.get("nom") or ""
                            adresse = r.get("adresse") or r.get("adresse_complete") or ""
                            ville = r.get("ville") or r.get("commune") or ""
                            full_addr = adresse or f"{denom} {cp} {ville}"
                            rows.append({"siren": siren, "denomination": denom, "adresse": adresse, "ville": ville, "full_addr": full_addr})

                        df = pd.DataFrame(rows).drop_duplicates(subset=["siren"])

                        # géocode + distance
                        with st.spinner("Géocodage + tri par distance…"):
                            lats, lons, dists = [], [], []
                            for a in df["full_addr"].tolist():
                                la, lo = geocode_one(a)
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

with right:
    st.subheader("3) Sélection (max 5) + téléchargement ZIP")

    df = st.session_state.get("results_df")
    if df is None or df.empty:
        st.info("Lance une recherche pour voir apparaître les 10 plus proches ici.")
    else:
        st.dataframe(df[["siren", "denomination", "adresse", "ville", "distance_km"]], use_container_width=True)

        options = df["siren"].tolist()

        # Sélection persistante : on ne réinitialise pas
        current = [s for s in st.session_state["selected_sirens"] if s in options]
        selected = st.multiselect(
            "Entreprises sélectionnées",
            options=options,
            default=current,
            max_selections=5,
            key="multisel_sirens",
        )
        st.session_state["selected_sirens"] = selected

        selected_rows = df[df["siren"].isin(selected)][["siren", "denomination"]].to_dict("records")

        disabled = (len(selected_rows) == 0) or (not TOKEN) or (not DEFAULT_RECIPIENT)
        if st.button("4) Télécharger les bilans (ZIP)", disabled=disabled):
            with st.spinner("Téléchargement des PDF + création du ZIP…"):
                zip_bytes = build_zip(selected_rows)
            st.download_button(
                "⬇️ Télécharger le ZIP",
                data=zip_bytes,
                file_name="bilans_selection.zip",
                mime="application/zip",
            )

        if not TOKEN or not DEFAULT_RECIPIENT:
            st.warning("Secrets manquants : API_ENTREPRISE_TOKEN et/ou DEFAULT_RECIPIENT.")

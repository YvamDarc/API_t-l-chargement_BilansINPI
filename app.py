import io
import re
import time
import zipfile
from typing import Dict, List, Tuple, Optional

import pandas as pd
import requests
import streamlit as st
from tenacity import retry, stop_after_attempt, wait_exponential

st.set_page_config(page_title="Sector → Carte → Comptes annuels (ZIP)", layout="wide")

SEARCH_API_BASE = "https://recherche-entreprises.api.gouv.fr"
GEO_ADRESSE_BASE = "https://api-adresse.data.gouv.fr"  # API Adresse via geo.api.gouv.fr
API_ENT_BASE = "https://entreprise.api.gouv.fr"

TOKEN = st.secrets.get("API_ENTREPRISE_TOKEN", "")
DEFAULT_CONTEXT = st.secrets.get("DEFAULT_CONTEXT", "comptes-annuels")
DEFAULT_RECIPIENT = st.secrets.get("DEFAULT_RECIPIENT", "")
DEFAULT_OBJECT = st.secrets.get("DEFAULT_OBJECT", "telechargement-comptes-annuels")

if not TOKEN:
    st.warning("⚠️ Ajoute API_ENTREPRISE_TOKEN dans les Secrets Streamlit Cloud.")
if not DEFAULT_RECIPIENT:
    st.warning("⚠️ Ajoute DEFAULT_RECIPIENT (SIRET organisme) dans les Secrets (ou passe-le en dur).")

def only_digits(s: str) -> str:
    return re.sub(r"\D", "", (s or "").strip())

@retry(stop=stop_after_attempt(4), wait=wait_exponential(min=1, max=10))
def http_get_json(url: str, headers: Optional[dict] = None, params: Optional[dict] = None, timeout: int = 30) -> dict:
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    # 429/5xx -> retry
    if r.status_code in (429, 500, 502, 503, 504):
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
    r.raise_for_status()
    return r.json()

@st.cache_data(ttl=1800, show_spinner=False)
def search_companies(q: str, code_postal: str, code_naf: str, page: int = 1, per_page: int = 25) -> dict:
    params = {
        "q": q,
        "code_postal": code_postal or None,
        "code_naf": code_naf or None,
        "page": page,
        "per_page": per_page,
    }
    # enlève les None (API plus tolérante)
    params = {k: v for k, v in params.items() if v}
    return http_get_json(f"{SEARCH_API_BASE}/search", params=params)

@st.cache_data(ttl=7 * 24 * 3600, show_spinner=False)
def geocode_one(address: str) -> Tuple[Optional[float], Optional[float]]:
    # Géocodage BAN / API Adresse
    params = {"q": address, "limit": 1}
    data = http_get_json(f"{GEO_ADRESSE_BASE}/search/", params=params)
    feats = data.get("features") or []
    if not feats:
        return None, None
    lon, lat = feats[0]["geometry"]["coordinates"]
    return float(lat), float(lon)

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_actes_bilans(siren: str) -> dict:
    url = f"{API_ENT_BASE}/v3/inpi/rne/unites_legales/open_data/{siren}/actes_bilans"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    params = {"context": DEFAULT_CONTEXT, "recipient": DEFAULT_RECIPIENT, "object": DEFAULT_OBJECT}
    return http_get_json(url, headers=headers, params=params)

@retry(stop=stop_after_attempt(4), wait=wait_exponential(min=1, max=10))
def download_pdf(url: str) -> bytes:
    headers = {"Authorization": f"Bearer {TOKEN}"}
    r = requests.get(url, headers=headers, timeout=60)
    if r.status_code in (429, 500, 502, 503, 504):
        raise RuntimeError(f"HTTP {r.status_code}")
    r.raise_for_status()
    return r.content

def build_zip_for_sirens(selected: List[Dict]) -> bytes:
    """
    selected: liste de dict contenant au minimum {"siren":..., "denomination":...}
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for item in selected:
            siren = item["siren"]
            name = (item.get("denomination") or "entreprise").replace("/", "-")[:80]
            # 1) liste des bilans/actes
            data = fetch_actes_bilans(siren)
            bilans = (data.get("bilans") or [])
            if not bilans:
                zf.writestr(f"{siren}_{name}/README.txt", "Aucun bilan public trouvé via actes_bilans.\n")
                continue

            # 2) download PDFs (on throttle un peu)
            for b in bilans:
                date_cloture = (b.get("date_cloture") or b.get("date_depot") or "date_inconnue")
                pdf_url = b.get("url") or b.get("url_document") or b.get("url_bilan")
                if not pdf_url:
                    continue
                time.sleep(0.25)  # anti-rafale côté API Entreprise
                pdf_bytes = download_pdf(pdf_url)
                zf.writestr(f"{siren}_{name}/bilan_{date_cloture}.pdf", pdf_bytes)
    buf.seek(0)
    return buf.read()

st.title("Entreprises par secteur + zone → carte → ZIP des comptes annuels")

with st.sidebar:
    st.subheader("Recherche")
    q = st.text_input("Mot-clé (optionnel)", value="")
    code_postal = st.text_input("Code postal (ex: 22000)", value="")
    code_naf = st.text_input("Code NAF (ex: 56.10A)", value="")
    per_page = st.slider("Résultats", 10, 50, 25, 5)
    launch = st.button("Rechercher")

if launch:
    with st.spinner("Recherche des entreprises…"):
        res = search_companies(q=q, code_postal=code_postal, code_naf=code_naf, page=1, per_page=per_page)

    results = res.get("results") or res.get("entreprises") or []
    if not results:
        st.info("Aucun résultat. Essaie un autre code postal / NAF.")
        st.stop()

    # Normalisation légère (selon champs renvoyés)
    rows = []
    for r in results:
        siren = only_digits(r.get("siren") or "")
        denom = r.get("denomination") or r.get("nom_complet") or r.get("nom") or ""
        adresse = r.get("adresse") or r.get("adresse_complete") or ""
        cp = r.get("code_postal") or code_postal
        ville = r.get("ville") or r.get("commune") or ""
        full_addr = adresse or f"{denom} {cp} {ville}"
        rows.append({"siren": siren, "denomination": denom, "adresse": adresse, "cp": cp, "ville": ville, "full_addr": full_addr})

    df = pd.DataFrame(rows).drop_duplicates(subset=["siren"]).head(per_page)

    # Géocode (limité, cache)
    with st.spinner("Géocodage pour la carte…"):
        lats, lons = [], []
        for a in df["full_addr"].tolist():
            lat, lon = geocode_one(a)
            lats.append(lat); lons.append(lon)
        df["lat"] = lats
        df["lon"] = lons

    left, right = st.columns([1.2, 1])

    with left:
        st.subheader("Résultats")
        st.dataframe(df[["siren", "denomination", "adresse", "cp", "ville"]], use_container_width=True)

        st.caption("Sélectionne jusqu’à 5 entreprises (par SIREN).")
        choices = df["siren"].tolist()
        selected_sirens = st.multiselect("SIREN sélectionnés", options=choices, default=choices[:1], max_selections=5)

        selected_rows = df[df["siren"].isin(selected_sirens)][["siren", "denomination"]].to_dict("records")

        if st.button("Télécharger les comptes annuels (ZIP)", disabled=(len(selected_rows) == 0 or not TOKEN or not DEFAULT_RECIPIENT)):
            with st.spinner("Téléchargement + création du ZIP…"):
                zip_bytes = build_zip_for_sirens(selected_rows)

            st.download_button(
                "⬇️ Télécharger le ZIP",
                data=zip_bytes,
                file_name="comptes_annuels_selection.zip",
                mime="application/zip",
            )

    with right:
        st.subheader("Carte")
        map_df = df.dropna(subset=["lat", "lon"]).copy()
        if map_df.empty:
            st.info("Pas de coordonnées trouvées pour afficher la carte.")
        else:
            st.pydeck_chart(
                {
                    "initialViewState": {
                        "latitude": float(map_df["lat"].mean()),
                        "longitude": float(map_df["lon"].mean()),
                        "zoom": 11,
                        "pitch": 0,
                    },
                    "layers": [
                        {
                            "type": "ScatterplotLayer",
                            "data": map_df,
                            "getPosition": "[lon, lat]",
                            "getRadius": 80,
                            "pickable": True,
                        }
                    ],
                    "tooltip": {"text": "{denomination}\nSIREN: {siren}\n{adresse} {cp} {ville}"},
                }
            )

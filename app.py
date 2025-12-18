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
st.set_page_config(page_title="Carte → entreprises proches → bilans INPI (ZIP)", layout="wide")

SEARCH_API_BASE = "https://recherche-entreprises.api.gouv.fr"
ADRESSE_BASE = "https://api-adresse.data.gouv.fr"

INPI_ENV = (st.secrets.get("INPI_ENV", "prod") or "prod").strip().lower()
INPI_USERNAME = (st.secrets.get("INPI_USERNAME", "") or "").strip()
INPI_PASSWORD = (st.secrets.get("INPI_PASSWORD", "") or "").strip()

if INPI_ENV == "pprod":
    INPI_BASE = "https://registre-national-entreprises-pprod.inpi.fr"
else:
    INPI_BASE = "https://registre-national-entreprises.inpi.fr"

# =========================
# Utils
# =========================
def only_digits(s: str) -> str:
    return re.sub(r"\D", "", (s or "").strip())

def _short(text: str, n=900) -> str:
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
# HTTP helpers
# =========================
@retry(stop=stop_after_attempt(4), wait=wait_exponential(min=1, max=10), reraise=True)
def get_json(url: str, headers=None, params=None, timeout=35) -> dict:
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
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
def post_json(url: str, json_body: dict, headers=None, timeout=35) -> dict:
    r = requests.post(url, json=json_body, headers=headers, timeout=timeout)
    ctype = (r.headers.get("content-type") or "").lower()
    payload = None
    if "application/json" in ctype:
        try:
            payload = r.json()
        except Exception:
            payload = None

    if r.status_code in (429, 500, 502, 503, 504):
        msg = payload if payload is not None else _short(r.text)
        raise RuntimeError(f"Transient HTTP {r.status_code} on {url} body={msg}")

    if r.status_code >= 400:
        msg = payload if payload is not None else _short(r.text)
        raise RuntimeError(f"HTTP {r.status_code} on {url} body={msg}")

    return payload if payload is not None else r.json()

@retry(stop=stop_after_attempt(4), wait=wait_exponential(min=1, max=10), reraise=True)
def download_bytes(url: str, headers=None, timeout=90) -> bytes:
    r = requests.get(url, headers=headers, timeout=timeout)
    if r.status_code in (429, 500, 502, 503, 504):
        raise RuntimeError(f"Transient download HTTP {r.status_code} for {url}")
    if r.status_code >= 400:
        raise RuntimeError(f"Download HTTP {r.status_code} for {url} body={_short(r.text)}")
    return r.content

# =========================
# BAN / Recherche Entreprises
# =========================
@st.cache_data(ttl=24 * 3600, show_spinner=False)
def reverse_postcode(lat: float, lon: float) -> Optional[str]:
    data = get_json(f"{ADRESSE_BASE}/reverse/", params={"lat": lat, "lon": lon}, timeout=20)
    feats = data.get("features") or []
    if not feats:
        return None
    return (feats[0].get("properties") or {}).get("postcode")

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
    per_page = max(1, min(int(per_page), 25))  # contrainte API
    params = {"code_postal": code_postal, "page": page, "per_page": per_page}
    if code_naf:
        params["code_naf"] = code_naf
    return get_json(f"{SEARCH_API_BASE}/search", params=params, timeout=35)

# =========================
# INPI Auth + endpoints (RNE)
# =========================
def inpi_login() -> str:
    """
    Login INPI : POST /api/sso/login => {token: "..."}
    Token à envoyer en Authorization: Bearer <token>
    """
    if not INPI_USERNAME or not INPI_PASSWORD:
        raise RuntimeError("Secrets INPI manquants : INPI_USERNAME / INPI_PASSWORD")

    url = f"{INPI_BASE}/api/sso/login"
    payload = {"username": INPI_USERNAME, "password": INPI_PASSWORD}
    data = post_json(url, payload, timeout=35)
    token = (data.get("token") or "").strip()
    if not token:
        raise RuntimeError("Login INPI OK mais token absent dans la réponse.")
    return token

def get_inpi_token(force: bool = False) -> str:
    """
    Stocke le token dans session_state. Si 401 plus tard, on force un relogin.
    """
    if force or not st.session_state.get("inpi_token"):
        st.session_state["inpi_token"] = inpi_login()
    return st.session_state["inpi_token"]

def inpi_headers() -> dict:
    token = get_inpi_token()
    return {"Authorization": f"Bearer {token}"}

def inpi_get_attachments(siren: str) -> dict:
    """
    GET /api/companies/{siren}/attachments => {actes:[], bilans:[], bilansSaisis:[]}
    """
    url = f"{INPI_BASE}/api/companies/{siren}/attachments"
    try:
        return get_json(url, headers=inpi_headers(), timeout=35)
    except RuntimeError as e:
        if "HTTP 401" in str(e):
            # relogin et retry 1x
            st.session_state["inpi_token"] = None
            return get_json(url, headers=inpi_headers(), timeout=35)
        raise

def inpi_download_bilan_pdf(bilan_id: str) -> bytes:
    """
    GET /api/bilans/{id}/download => PDF (binaire)
    """
    url = f"{INPI_BASE}/api/bilans/{bilan_id}/download"
    try:
        return download_bytes(url, headers=inpi_headers(), timeout=120)
    except RuntimeError as e:
        if "HTTP 401" in str(e) or "Download HTTP 401" in str(e):
            st.session_state["inpi_token"] = None
            return download_bytes(url, headers=inpi_headers(), timeout=120)
        raise

def build_zip_inpi(selected: List[Dict]) -> bytes:
    """
    Pour chaque SIREN :
      - attachments -> bilans
      - download pdf for each bilan id (non deleted)
      - zip
    """
    buf = io.BytesIO()

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for ent in selected:
            siren = ent["siren"]
            name = (ent.get("denomination") or "entreprise").replace("/", "-").replace("\\", "-")[:80]
            folder = f"{siren}_{name}"

            try:
                att = inpi_get_attachments(siren)
            except Exception as e:
                zf.writestr(f"{folder}/README_erreur.txt", f"Erreur attachments INPI: {e}\n")
                continue

            bilans = att.get("bilans") or []
            # NB: bilansSaisis = données structurées, bilans = pdf (métadonnées + id) :contentReference[oaicite:5]{index=5}
            if not bilans:
                zf.writestr(f"{folder}/README.txt", "Aucun bilan trouvé (attachements.bilans vide).\n")
                continue

            count_ok = 0
            for b in bilans:
                if b.get("deleted") is True:
                    continue
                bilan_id = (b.get("id") or "").strip()
                if not bilan_id:
                    continue

                date_cloture = b.get("dateCloture") or "date_inconnue"
                date_depot = b.get("dateDepot") or "depot_inconnu"
                confidentiality = b.get("confidentiality") or "Unknown"
                filename = f"bilan_{date_cloture}_depot_{date_depot}_{confidentiality}.pdf"
                filename = filename.replace(" ", "_").replace("/", "-")

                # anti-rafale (quota et confort)
                time.sleep(0.25)

                try:
                    pdf_bytes = inpi_download_bilan_pdf(bilan_id)
                    zf.writestr(f"{folder}/{filename}", pdf_bytes)
                    count_ok += 1
                except Exception as e:
                    zf.writestr(f"{folder}/ERREUR_{bilan_id}.txt", f"Erreur download bilan: {e}\n")

            if count_ok == 0:
                zf.writestr(f"{folder}/README.txt", "Bilans présents mais aucun PDF téléchargé (erreurs/accès/confidentialité).\n")

    buf.seek(0)
    return buf.read()

# =========================
# Session state
# =========================
st.session_state.setdefault("click_latlon", None)
st.session_state.setdefault("results_df", None)
st.session_state.setdefault("selected_sirens", [])
st.session_state.setdefault("last_cp", None)
st.session_state.setdefault("inpi_token", None)

# =========================
# UI
# =========================
st.title("Carte → entreprises proches → ZIP des comptes annuels (INPI)")

with st.sidebar:
    st.subheader("INPI / RNE")
    st.write("Environnement :", INPI_ENV)
    st.write("INPI_USERNAME présent :", bool(INPI_USERNAME))
    st.write("INPI_PASSWORD présent :", bool(INPI_PASSWORD))
    if st.button("Tester login INPI"):
        try:
            t = get_inpi_token(force=True)
            st.success(f"Login OK (token reçu, longueur={len(t)})")
        except Exception as e:
            st.error("Login INPI KO")
            st.exception(e)
    st.caption("Le login/token et les routes bilans sont décrits dans la doc INPI API comptes annuels. :contentReference[oaicite:6]{index=6}")

left, right = st.columns([1.25, 1])

with left:
    st.subheader("1) Clique sur la carte (point de recherche)")
    naf = normalize_naf(st.text_input("NAF (optionnel) — ex: 56.10A", value=""))

    candidates_per_page = st.slider("Pool candidat (max 25)", 10, 25, 25, 5)
    use_two_pages = st.checkbox("2 pages (jusqu’à 50 candidats)", value=True)

    default_center = st.session_state["click_latlon"] or (48.5, -2.8)
    m = folium.Map(location=default_center, zoom_start=10, control_scale=True)
    if st.session_state["click_latlon"]:
        folium.Marker(st.session_state["click_latlon"], tooltip="Point",
                      icon=folium.Icon(color="red")).add_to(m)
    map_state = st_folium(m, height=520, width=None)
    if map_state and map_state.get("last_clicked"):
        st.session_state["click_latlon"] = (map_state["last_clicked"]["lat"], map_state["last_clicked"]["lng"])

    if st.button("2) Trouver les 10 entreprises les plus proches", type="primary"):
        if not st.session_state["click_latlon"]:
            st.warning("Clique d’abord sur la carte.")
        else:
            lat, lon = st.session_state["click_latlon"]
            cp = reverse_postcode(lat, lon)
            if not cp:
                st.error("Impossible de déterminer un code postal ici.")
                st.stop()
            st.session_state["last_cp"] = cp

            with st.spinner(f"Recherche entreprises (CP {cp})…"):
                res1 = search_companies_by_cp(cp, naf, per_page=candidates_per_page, page=1)
                results = (res1.get("results") or res1.get("entreprises") or [])
                if use_two_pages:
                    res2 = search_companies_by_cp(cp, naf, per_page=candidates_per_page, page=2)
                    results += (res2.get("results") or res2.get("entreprises") or [])

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
                st.session_state["results_df"] = pd.DataFrame()
            else:
                with st.spinner("Géocodage + tri distance…"):
                    lats, lons, dists = [], [], []
                    for addr in df["full_addr"].tolist():
                        la, lo = geocode_addr(addr)
                        lats.append(la); lons.append(lo)
                        dists.append(haversine_km(lat, lon, la, lo) if la is not None and lo is not None else 10**9)
                    df["lat"] = lats
                    df["lon"] = lons
                    df["distance_km"] = dists
                    df = df.sort_values("distance_km").head(10).reset_index(drop=True)

                st.session_state["results_df"] = df
                allowed = set(df["siren"].tolist())
                st.session_state["selected_sirens"] = [s for s in st.session_state["selected_sirens"] if s in allowed]

    if st.session_state["last_cp"]:
        st.caption(f"CP détecté : **{st.session_state['last_cp']}**")

with right:
    st.subheader("3) Sélection (max 5) + téléchargement ZIP (INPI)")

    df = st.session_state.get("results_df")
    if df is None:
        st.info("Clique sur la carte puis lance la recherche.")
        st.stop()
    if df.empty:
        st.info("Pas de résultats.")
        st.stop()

    st.dataframe(df[["siren", "denomination", "adresse", "ville", "distance_km"]], use_container_width=True)

    options = df["siren"].tolist()
    default_sel = [s for s in st.session_state["selected_sirens"] if s in options]
    selected = st.multiselect("Entreprises sélectionnées", options=options, default=default_sel, max_selections=5)
    st.session_state["selected_sirens"] = selected

    selected_rows = df[df["siren"].isin(selected)][["siren", "denomination"]].to_dict("records")

    dl_disabled = (len(selected_rows) == 0) or (not INPI_USERNAME) or (not INPI_PASSWORD)
    if st.button("4) Télécharger les comptes annuels (ZIP)", disabled=dl_disabled):
        try:
            with st.spinner("Login INPI + récupération des bilans + création du ZIP…"):
                zip_bytes = build_zip_inpi(selected_rows)
            st.download_button("⬇️ Télécharger le ZIP", data=zip_bytes,
                               file_name="comptes_annuels_inpi.zip", mime="application/zip")
        except Exception as e:
            st.error("Erreur lors de la création du ZIP INPI.")
            st.exception(e)

    if not INPI_USERNAME or not INPI_PASSWORD:
        st.warning("Secrets INPI manquants : INPI_USERNAME / INPI_PASSWORD.")

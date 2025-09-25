import os, sys, subprocess, tempfile, time, json 
from io import BytesIO
from datetime import datetime, date, time as dtime, timedelta
import xml.etree.ElementTree as ET
import pandas as pd
import streamlit as st

import platform, pkgutil
st.caption(f"Python: {platform.python_version()}")
st.caption(f"openpyxl present: {bool(pkgutil.find_loader('openpyxl'))}")


# =============================================================
# Zelfstart (kort & solide): druk op Play in VS Code -> 1× streamlit run
# =============================================================

def _is_streamlit():
    return any(k.startswith("STREAMLIT_") for k in os.environ) or os.environ.get("STREAMLIT_BOOTSTRAPPED") == "1"

def _lock_path():
    return os.path.join(tempfile.gettempdir(), f"sl_{abs(hash(os.path.abspath(__file__)))}.pid")

def _alive(pid:int) -> bool:
    try:
        if pid <= 0: return False
        os.kill(pid, 0)
        return True
    except Exception:
        return False

if __name__ == "__main__" and not _is_streamlit():
    lp = _lock_path()
    if os.path.exists(lp):
        try:
            with open(lp, "r", encoding="utf-8") as f: pid = int((f.read() or "0").strip())
        except Exception: pid = 0
        if _alive(pid):
            print("Streamlit draait al – geen nieuwe instance gestart.")
            sys.exit(0)
    cmd = [sys.executable, "-m", "streamlit", "run", os.path.abspath(__file__), "--server.fileWatcherType=none"]
    env = {**os.environ, "STREAMLIT_BOOTSTRAPPED": "1"}
    p = subprocess.Popen(cmd, env=env)
    try:
        with open(_lock_path(), "w", encoding="utf-8") as f: f.write(str(p.pid))
    except Exception:
        pass
    time.sleep(1)
    sys.exit(0)

# ===========================
# Streamlit app
# ===========================

st.set_page_config(page_title="Uren & Verlof – Correcties", layout="wide")

# ------------------------------
# Inlogscherm (toegevoegd)
# ------------------------------
st.session_state.setdefault("auth_ok", False)
if not st.session_state["auth_ok"]:
    st.title("Inloggen")
    with st.form("login_form", clear_on_submit=False):
        u = st.text_input("Gebruikersnaam")
        p = st.text_input("Wachtwoord", type="password")
        ok = st.form_submit_button("Inloggen")
    if ok:
        if u == "HR" and p == "1234":
            st.session_state["auth_ok"] = True
            st.rerun()
        else:
            st.error("Onjuiste inloggegevens.")
    st.stop()
# ------------------------------
# Einde inlogscherm
# ------------------------------

# Session defaults (één keer)
st.session_state.setdefault("use_dev", False)
st.session_state.setdefault("dev_xlsx_path", "")
st.session_state.setdefault("dev_xml_path", "")
st.session_state.setdefault("period_base", os.path.join(os.path.dirname(os.path.abspath(__file__)), "perioden"))
st.session_state.setdefault("periode", datetime.now().strftime("%Y-%m"))

if "corrections" not in st.session_state: st.session_state.corrections = {}
if "medewerkers_76" not in st.session_state:
    st.session_state.medewerkers_76 = {"Hol, G (Gerwin)", "Jong, J.J. de (John)", "Leeuwen, JE van (Jeroen)"}
if "norm_per_dag" not in st.session_state:
    # Naam -> uren per dag (float). Wordt ingevuld zodra data is ingelezen.
    st.session_state.norm_per_dag = {}

# ------------------------------
# Helpers
# ------------------------------

def werkbare_dagen(start: datetime, eind: datetime):
    cur = start
    while cur <= eind:
        if cur.weekday() < 5: yield cur
        cur += timedelta(days=1)

@st.cache_data(show_spinner=False)
def read_input_xlsx(src) -> pd.DataFrame:
    if src is None or (isinstance(src, str) and not src.strip()): return pd.DataFrame()
    return pd.read_excel(src, dtype=str).drop_duplicates()

@st.cache_data(show_spinner=False)
def extract_verlof(xml_src) -> pd.DataFrame:
    if xml_src is None or (isinstance(xml_src, str) and not xml_src.strip()): return pd.DataFrame()
    try:
        tree = ET.parse(xml_src)
    except Exception:
        return pd.DataFrame()
    root = tree.getroot(); ns = {'ns': root.tag.split('}')[0].strip('{')}
    rec = []
    for grp in root.findall('.//ns:grpExtra1', ns):
        naam = grp.attrib.get('Naam','').strip()
        for d in grp.findall('.//ns:Detail', ns):
            uren = float(d.attrib.get('OpgenomenOpname') or d.attrib.get('ActueelOpname') or 0)
            if uren <= 0: continue
            s, e = d.attrib.get('Startdatum'), d.attrib.get('Einddatum') or d.attrib.get('Startdatum')
            try:
                d0, d1 = datetime.fromisoformat(s[:10]), datetime.fromisoformat(e[:10])
            except Exception:
                continue
            dagen = list(werkbare_dagen(d0, d1)); per_dag = min(8, uren/len(dagen)) if dagen else 0
            for dag in dagen:
                rec.append({
                    'Naam SD WORX': naam,
                    'Datum': pd.to_datetime(dag.date()),
                    'Weeknummer': dag.isocalendar().week,
                    'Verlof_uren': round(per_dag,2),
                    'Verlof_type': d.attrib.get('Verlofsoort','').strip()
                })
    dfv = pd.DataFrame(rec)
    if not dfv.empty: dfv.sort_values(['Naam SD WORX','Datum'], inplace=True)
    return dfv


def calc_uren_per_dag(df_input: pd.DataFrame, medewerkers_76_uren:set, norm_map:dict|None=None) -> pd.DataFrame:
    if df_input is None or (isinstance(df_input, pd.DataFrame) and df_input.empty):
        return pd.DataFrame()
    df = df_input.copy()
    df['Datum'] = pd.to_datetime(df['Datum'], errors='coerce')
    df['Weeknummer'] = df['Datum'].dt.isocalendar().week
    df['Start'] = pd.to_datetime(df['Start'], format='%H:%M:%S', errors='coerce').dt.time
    df['Eind']  = pd.to_datetime(df['Eind'],  format='%H:%M:%S', errors='coerce').dt.time
    df['Minuten'] = pd.to_timedelta(df['Minuten'].astype(str), errors='coerce')
    df['Start_dt'] = df.apply(lambda r: pd.Timestamp.combine(r['Datum'].date(), r['Start']) if pd.notnull(r['Start']) else pd.NaT, axis=1)
    df['Eind_dt']  = df.apply(lambda r: pd.Timestamp.combine(r['Datum'].date(), r['Eind'])  if pd.notnull(r['Eind'])  else pd.NaT, axis=1)

    pauze_df = df[df['Kloktype'].str.lower()=='pauze'].copy()
    pauze_df['Pauze_minuten'] = pauze_df['Minuten'].dt.total_seconds()/60
    pauze = pauze_df.groupby(['Naam SD WORX','Datum'], as_index=False)['Pauze_minuten'].sum()

    se = df.groupby(['Naam SD WORX','Datum'], as_index=False).agg(Eerste_start=('Start_dt','min'), Laatste_eind=('Eind_dt','max'))
    se = se.merge(df[['Naam SD WORX','Datum','Weeknummer']].drop_duplicates(), on=['Naam SD WORX','Datum'], how='left')

    orig = df[['Naam SD WORX','Datum','Start_dt']]
    def fix_start(r):
        t = r['Eerste_start']
        if pd.notnull(t) and dtime(0,0,1) <= t.time() <= dtime(0,4,0):
            c = orig[(orig['Naam SD WORX']==r['Naam SD WORX']) & (orig['Datum']==r['Datum']) & (orig['Start_dt'].dt.time>=dtime(0,4,0))]
            if not c.empty: return c['Start_dt'].min()
        return t
    se['Eerste_start'] = se.apply(fix_start, axis=1)

    se['Dienstduur_minuten'] = (se['Laatste_eind'] - se['Eerste_start']).dt.total_seconds()/60
    res = se.merge(pauze, on=['Naam SD WORX','Datum'], how='left').fillna({'Pauze_minuten':0})

    def type_dienst(r):
        wd = r['Datum'].weekday()
        if wd==5: return 'Zaterdag dienst'
        if wd==6: return 'Zondag dienst'
        if pd.notnull(r['Eerste_start']) and pd.notnull(r['Laatste_eind']) and r['Eerste_start'].time()>dtime(11,0) and r['Dienstduur_minuten']>=300:
            return 'Avond dienst'
        return 'Reguliere dienst'
    res['Type_dienst'] = res.apply(type_dienst, axis=1)

    def soll(r):
        if r['Type_dienst'] in ['Zaterdag dienst','Zondag dienst']: return 15 if r['Dienstduur_minuten']>0 else 0
        return 60 if r['Dienstduur_minuten']>400 else r['Pauze_minuten']
    res['Soll_pauze'] = res.apply(soll, axis=1)

    def corr(r):
        if r['Type_dienst'] in ['Reguliere dienst','Avond dienst'] and r['Soll_pauze']>=45 and r['Pauze_minuten']<39: return 30
        return 0
    res['Te_corrigeren_pauze'] = res.apply(corr, axis=1)

    def force(r):
        tot = r['Pauze_minuten'] + r['Te_corrigeren_pauze']
        if r['Dienstduur_minuten']>420 and tot<60: return max(0,60-r['Pauze_minuten'])
        return r['Te_corrigeren_pauze']
    res['Te_corrigeren_pauze'] = res.apply(force, axis=1)

    # Norm per medewerker (minuten)
    def _norm_minutes(name:str) -> float:
        if isinstance(norm_map, dict) and name in norm_map:
            try:
                return float(norm_map[name]) * 60.0
            except Exception:
                return (7.6*60 if name in medewerkers_76_uren else 8*60)
        return 7.6*60 if name in medewerkers_76_uren else 8*60
    res['Norm_werkdag_minuten'] = res['Naam SD WORX'].apply(_norm_minutes)

    # Netto te verlonen en raw
    res['Te_verlonen_minuten'] = (res['Dienstduur_minuten'] - res['Pauze_minuten'] - res['Te_corrigeren_pauze']).clip(lower=0)
    res['Te_verlonen_minuten'] = res.apply(lambda r: min(r['Te_verlonen_minuten'], r['Norm_werkdag_minuten']), axis=1)
    res['Raw_minutes'] = (res['Dienstduur_minuten'] - res['Pauze_minuten'] - res['Te_corrigeren_pauze']).clip(lower=0)

    # Uren verdelen t.o.v. persoonlijke norm (in plaats van vaste 8h/10h)
    special = ['Bezuijen, P. C. (Piet)','Reijns, M. (Michiel)','Steen, S. van der (Stefan)','Vos, D. M. (Marcel)']
    def split_row(r):
        # Weekend: nooit reguliere/overuren—die tellen uitsluitend als zaterdag/zondag
        if r['Type_dienst'] in ('Zaterdag dienst', 'Zondag dienst'):
            return pd.Series({'Reguliere_uren': 0.0, 'Uren_128': 0.0, 'Uren_147': 0.0})

        m = float(r['Raw_minutes'])
        nm = float(r['Norm_werkdag_minuten'])  # persoonlijke norm in minuten
        regulier = min(m, nm)

        if r['Naam SD WORX'] in special:
            uren128 = 0
            uren147 = max(m - nm, 0)
        else:
            over = max(m - nm, 0)
            uren128 = min(over, 120)
            uren147 = max(over - 120, 0)

        return pd.Series({
            'Reguliere_uren': round(regulier/60, 2),
            'Uren_128': round(uren128/60, 2),
            'Uren_147': round(uren147/60, 2)
        })

    res = pd.concat([res.reset_index(drop=True), res.apply(split_row, axis=1).reset_index(drop=True)], axis=1)

    def toeslag(r):
        if r['Type_dienst']=='Avond dienst' and pd.notnull(r['Laatste_eind']) and r['Laatste_eind'].time()>dtime(19,0):
            return ((pd.Timestamp.combine(date.today(), r['Laatste_eind'].time()) - pd.Timestamp.combine(date.today(), max(r['Eerste_start'].time(), dtime(19,0)))).total_seconds()/3600)
        return 0
    res['Toeslag_uren'] = res.apply(toeslag, axis=1)
    res['Zaterdag_uren'] = res.apply(lambda r: r['Te_verlonen_minuten']/60 if r['Type_dienst']=='Zaterdag dienst' else 0, axis=1)
    res['Zondag_uren']   = res.apply(lambda r: r['Te_verlonen_minuten']/60 if r['Type_dienst']=='Zondag dienst' else 0, axis=1)
    res['Ziekte_uren'] = 0.0
    return res


def apply_corrections(df: pd.DataFrame, corr: dict) -> pd.DataFrame:
    if df.empty or not corr: return df
    out = df.copy(); out['__d'] = out['Datum'].dt.date
    for (naam, dag), c in corr.items():
        m = (out['Naam SD WORX']==naam) & (out['__d']==dag)
        if not m.any():
            new = {'Naam SD WORX': naam, 'Datum': pd.Timestamp(dag), 'Weeknummer': pd.Timestamp(dag).isocalendar().week,
                   'Eerste_start': pd.NaT, 'Laatste_eind': pd.NaT, 'Pauze_minuten':0.0, 'Type_dienst': c.get('Type_dienst','Reguliere dienst'),
                   'Soll_pauze':0.0,'Te_corrigeren_pauze':0.0,'Norm_werkdag_minuten':480,'Te_verlonen_minuten':0.0,'Raw_minutes':0.0,
                   'Reguliere_uren':0.0,'Uren_128':0.0,'Uren_147':0.0,'Toeslag_uren':0.0,'Zaterdag_uren':0.0,'Zondag_uren':0.0,
                   'Ziekte_uren':0.0,'Verlof_uren':0.0}
            out = pd.concat([out, pd.DataFrame([new])], ignore_index=True); m = (out['Naam SD WORX']==naam) & (out['__d']==dag)
        for k,v in c.items():
            if k in out.columns: out.loc[m, k] = v
    return out.drop(columns=['__d'])


def build_overviews(df_all: pd.DataFrame, norm_map:dict|None=None):
    if df_all.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    # Zorg dat alle benodigde kolommen bestaan
    df_all = df_all.copy()
    needed = ['Reguliere_uren','Uren_128','Uren_147','Toeslag_uren','Zaterdag_uren','Zondag_uren','Verlof_uren','Ziekte_uren']
    for c in needed:
        if c not in df_all.columns:
            df_all[c] = 0.0

    week_cols = needed
    df_week = df_all.groupby(['Werkplaats','Naam SD WORX','Weeknummer'], as_index=False)[week_cols].sum()
    workdays = (
        df_all[df_all['Datum'].dt.weekday<5]
        .drop_duplicates(['Werkplaats','Naam SD WORX','Datum'])
        .groupby(['Werkplaats','Naam SD WORX','Weeknummer'], as_index=False)
        .size().rename(columns={'size':'Workdays'})
    )
    df_week = df_week.merge(workdays, on=['Werkplaats','Naam SD WORX','Weeknummer'], how='left').fillna({'Workdays':0})

    def soll_w(r):
        # Persoonlijke norm × werkdagen
        naam = r['Naam SD WORX']
        default = 7.6 if (naam in st.session_state.get('medewerkers_76', set())) else 8.0
        norm = None
        if isinstance(norm_map, dict):
            norm = norm_map.get(naam, default)
        if norm is None:
            norm = default
        return r['Workdays'] * float(norm)

    df_week['Soll_uren'] = df_week.apply(soll_w, axis=1)
    df_week['Totaal_excl_toeslag'] = (
        df_week['Reguliere_uren'] + df_week['Uren_128'] + df_week['Uren_147'] + df_week['Verlof_uren']
    )

    dfm = df_all.copy()
    dfm['Maand'] = dfm['Datum'].dt.to_period('M').astype(str)
    df_month = dfm.groupby(['Werkplaats','Naam SD WORX','Maand'], as_index=False)[week_cols].sum()

    wd_m = (
        dfm[dfm['Datum'].dt.weekday<5]
        .drop_duplicates(['Werkplaats','Naam SD WORX','Datum'])
        .groupby(['Werkplaats','Naam SD WORX','Maand'], as_index=False)
        .size().rename(columns={'size':'Workdays'})
    )
    df_month = df_month.merge(wd_m, on=['Werkplaats','Naam SD WORX','Maand'], how='left').fillna({'Workdays':0})

    def soll_m(r):
        naam = r['Naam SD WORX']
        default = 7.6 if (naam in st.session_state.get('medewerkers_76', set())) else 8.0
        norm = None
        if isinstance(norm_map, dict):
            norm = norm_map.get(naam, default)
        if norm is None:
            norm = default
        return r['Workdays'] * float(norm)

    df_month['Soll_uren'] = df_month.apply(soll_m, axis=1)
    df_month['Totaal_excl_toeslag'] = (
        df_month['Reguliere_uren'] + df_month['Uren_128'] + df_month['Uren_147'] + df_month['Verlof_uren']
    )
    df_month['Verschil'] = df_month['Totaal_excl_toeslag'] - df_month['Soll_uren']

    rows = []
    for _, r in df_month.iterrows():
        naam, diff = r['Naam SD WORX'], r['Verschil']
        if r['Zaterdag_uren']>0:
            rows.append({'Werkplaats': r['Werkplaats'], 'Naam SD WORX': naam, 'Uren': round(r['Zaterdag_uren'],1), 'Code':'32XXZAT'})
        if r['Zondag_uren']>0:
            rows.append({'Werkplaats': r['Werkplaats'], 'Naam SD WORX': naam, 'Uren': round(r['Zondag_uren'],1), 'Code':'32XXZON'})
        if r['Toeslag_uren']>0:
            rows.append({'Werkplaats': r['Werkplaats'], 'Naam SD WORX': naam, 'Uren': round(r['Toeslag_uren'],1), 'Code':'3245'})
        if diff < 0:
            rows.append({'Werkplaats': r['Werkplaats'], 'Naam SD WORX': naam, 'Uren': round(diff,1), 'Code':'Ziekte/verlof afboeken'})
        elif abs(diff) < 1:
            rows.append({'Werkplaats': r['Werkplaats'], 'Naam SD WORX': naam, 'Uren': 0, 'Code':'UREN EXACT KLOPPEND'})
        else:
            if r['Uren_128']>0:
                rows.append({'Werkplaats': r['Werkplaats'], 'Naam SD WORX': naam, 'Uren': round(r['Uren_128'],1), 'Code':'3211'})
            if r['Uren_147']>0:
                rows.append({'Werkplaats': r['Werkplaats'], 'Naam SD WORX': naam, 'Uren': round(r['Uren_147'],1), 'Code':'3212'})

    df_input_sd = pd.DataFrame(rows)
    return df_input_sd, df_week, df_month


def write_excel_bytes(df_uren, df_verlof, df_all, df_week, df_month, df_input_sd) -> bytes:
    bio = BytesIO()
    with pd.ExcelWriter(bio, engine='openpyxl') as w:
        df_input_sd.to_excel(w, index=False, sheet_name='INPUT SD WORX')
        df_uren.to_excel(w, index=False, sheet_name='Uren_per_dag')
        if df_verlof is not None and not df_verlof.empty: df_verlof.to_excel(w, index=False, sheet_name='Verlof_per_dag')
        df_all.to_excel(w, index=False, sheet_name='Uren_plus_Verlof')
        df_week.to_excel(w, index=False, sheet_name='WeekTotaal')
        df_month.to_excel(w, index=False, sheet_name='MaandTotaal')
    bio.seek(0); return bio.read()

# ------------------------------
# Periode opslag helpers
# ------------------------------

def _period_dir(base_dir: str, period: str) -> str:
    p = os.path.join(base_dir, period)
    os.makedirs(p, exist_ok=True)
    return p

def _safe_write_bytes(path: str, data: bytes):
    with open(path, "wb") as f:
        f.write(data)

def save_period(base_dir: str, period: str,
                upl_xlsx, dev_xlsx_path: str,
                upl_xml, dev_xml_path: str,
                df_input, df_verlof, df_all, df_week, df_month, df_input_sd,
                norm_map: dict, corrections: dict):
    """Slaat inputs, berekende tabellen en instellingen op in <base>/<period>/"""
    pdir = _period_dir(base_dir, period)

    # 1) Input-bestanden
    in_xlsx_path = os.path.join(pdir, "input.xlsx")
    if upl_xlsx is not None:
        _safe_write_bytes(in_xlsx_path, upl_xlsx.getvalue())
    elif dev_xlsx_path:
        bio = BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as w:
            (df_input if df_input is not None else pd.DataFrame()).to_excel(w, index=False, sheet_name="Sheet1")
        _safe_write_bytes(in_xlsx_path, bio.getvalue())

    xml_path = os.path.join(pdir, "verlof.xml")
    if upl_xml is not None:
        _safe_write_bytes(xml_path, upl_xml.getvalue())
    elif dev_xml_path:
        try:
            with open(dev_xml_path, "rb") as f:
                _safe_write_bytes(xml_path, f.read())
        except Exception:
            pass  # XML is optioneel

    # 2) Berekende data
    (df_all if df_all is not None else pd.DataFrame()).to_csv(os.path.join(pdir, "uren_plus_verlof.csv"), index=False)
    (df_week if df_week is not None else pd.DataFrame()).to_csv(os.path.join(pdir, "weektotaal.csv"), index=False)
    (df_month if df_month is not None else pd.DataFrame()).to_csv(os.path.join(pdir, "maandtotaal.csv"), index=False)
    (df_input_sd if df_input_sd is not None else pd.DataFrame()).to_csv(os.path.join(pdir, "sdwrox_input.csv"), index=False)

    # 3) Output Excel (zelfde als download)
    out_xlsx = write_excel_bytes(
        df_uren=(df_all if df_all is not None else pd.DataFrame()),
        df_verlof=(df_verlof if df_verlof is not None else pd.DataFrame()),
        df_all=(df_all if df_all is not None else pd.DataFrame()),
        df_week=(df_week if df_week is not None else pd.DataFrame()),
        df_month=(df_month if df_month is not None else pd.DataFrame()),
        df_input_sd=(df_input_sd if df_input_sd is not None else pd.DataFrame())
    )
    _safe_write_bytes(os.path.join(pdir, "output.xlsx"), out_xlsx)

    # 4) Instellingen (normen + correcties)
    serial_corr = {}
    for (naam, dag), payload in corrections.items():
        serial_corr.setdefault(naam, {})[str(dag)] = payload
    with open(os.path.join(pdir, "settings.json"), "w", encoding="utf-8") as f:
        json.dump({
            "norm_per_dag": norm_map,
            "corrections": serial_corr
        }, f, ensure_ascii=False, indent=2)

def load_period_inputs(base_dir: str, period: str):
    """Geeft paden terug naar input.xlsx / verlof.xml en eventueel settings."""
    pdir = os.path.join(base_dir, period)
    in_xlsx = os.path.join(pdir, "input.xlsx")
    xml = os.path.join(pdir, "verlof.xml")
    settings = os.path.join(pdir, "settings.json")

    has_xlsx = os.path.exists(in_xlsx)
    has_xml = os.path.exists(xml)
    norms = {}
    corr = {}
    if os.path.exists(settings):
        try:
            with open(settings, "r", encoding="utf-8") as f:
                j = json.load(f)
            norms = j.get("norm_per_dag", {})
            # Unflatten corrections
            for naam, byday in j.get("corrections", {}).items():
                for day_str, payload in byday.items():
                    y, m, d = map(int, day_str.split("-"))
                    corr[(naam, date(y, m, d))] = payload
        except Exception:
            pass
    return (in_xlsx if has_xlsx else ""), (xml if has_xml else ""), norms, corr

# ------------------------------
# UI – Sidebar (Periode & opslag eerst)
# ------------------------------

st.sidebar.header("Periode & opslag")

# Opslagmap
base_dir = st.sidebar.text_input("Opslagmap", value=st.session_state["period_base"], help="Hier worden per-periode bestanden en instellingen opgeslagen.")
st.session_state["period_base"] = base_dir

# Bestaande periodes (submappen)
existing_periods = []
try:
    if os.path.exists(base_dir):
        existing_periods = sorted([d for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))])
except Exception:
    pass

sel_existing = st.sidebar.selectbox("Bestaande periodes", ["(Geen)"] + existing_periods)
manual_period = st.sidebar.text_input("Periode handmatig (YYYY-MM)", value=st.session_state["periode"])
# Welke periode gebruiken?
periode = manual_period.strip() if manual_period.strip() else (sel_existing if sel_existing != "(Geen)" else datetime.now().strftime("%Y-%m"))
st.session_state["periode"] = periode

col_p1, col_p2 = st.sidebar.columns(2)
with col_p1:
    load_period_btn = st.button("📂 Laad periode")
with col_p2:
    save_period_btn_sidebar = st.button("💾 Opslaan periode")

st.sidebar.divider()
st.sidebar.header("Bestanden & filters")

upl_xlsx = st.sidebar.file_uploader("Uren INPUT.xlsx", type=["xlsx"], accept_multiple_files=False)
upl_xml  = st.sidebar.file_uploader("Verlof XML (optioneel)", type=["xml"], accept_multiple_files=False)

with st.sidebar.expander("Ontwikkelmodus (lokale paden)", expanded=False):
    st.session_state.use_dev = st.checkbox("Gebruik lokale paden", value=st.session_state.use_dev)
    _xlsx_tmp = st.text_input("Pad naar Uren INPUT.xlsx", value=st.session_state.dev_xlsx_path)
    _xml_tmp  = st.text_input("Pad naar Verlof XML (optioneel)", value=st.session_state.dev_xml_path)
    if _xlsx_tmp != st.session_state.dev_xlsx_path:
        st.session_state.dev_xlsx_path = _xlsx_tmp
    if _xml_tmp != st.session_state.dev_xml_path:
        st.session_state.dev_xml_path = _xml_tmp

exclude_default = ['Springer, J. P. (Jan)','Fictief']
exclude = st.sidebar.text_area("Uit te sluiten namen (‘;’ gescheiden)", "; ".join(exclude_default)).split(";") if st.sidebar.checkbox("Aangepaste uitsluitlijst", value=False) else exclude_default
exclude = [x.strip() for x in exclude]

st.sidebar.subheader("Medewerkers 7.6 u/dag")
med_76 = st.sidebar.text_area(
    "Namen (\n gescheiden)",
    value="\n".join(sorted(st.session_state.medewerkers_76))
)
if st.sidebar.button("Opslaan 7.6u-lijst"):
    st.session_state.medewerkers_76 = set(x.strip() for x in med_76.splitlines() if x.strip())
    st.sidebar.success("Opgeslagen.")

# ------------------------------
# Laad periode actie
# ------------------------------

if load_period_btn:
    per_xlsx, per_xml, norms, corr = load_period_inputs(st.session_state["period_base"], st.session_state["periode"])

    # Zet alles in session_state zodat de rest van het script het gebruikt
    st.session_state.use_dev = bool(per_xlsx or per_xml)
    st.session_state.dev_xlsx_path = per_xlsx or ""
    st.session_state.dev_xml_path  = per_xml  or ""

    if norms:
        st.session_state.norm_per_dag = norms
    if corr:
        st.session_state.corrections = corr

    if not per_xlsx:
        st.warning("Geen 'input.xlsx' gevonden in de geselecteerde periode-map. Upload een bestand of kies een andere periode.")
    else:
        st.success(f"Periode geladen uit: {os.path.join(st.session_state['period_base'], st.session_state['periode'])}")

    # Direct herberekenen met de nieuwe paden
    st.rerun()

# ------------------------------
# Main – Tabs & Dataflow
# ------------------------------

st.title("Uren, Verlof & Correcties")

# ---- Veiligheids-initialisatie zodat NameError niet kan optreden ----
df_input = pd.DataFrame()
df_verlof = pd.DataFrame()
df_uren = pd.DataFrame()
wp_map = pd.DataFrame(columns=['Naam SD WORX','Werkplaats'])

df_all = pd.DataFrame()
df_week = pd.DataFrame()
df_month = pd.DataFrame()
df_input_sd = pd.DataFrame()
# --------------------------------------------------------------------

# Als er geen upload is én geen dev-pad, dan stoppen
if not upl_xlsx and not (st.session_state.use_dev and st.session_state.dev_xlsx_path):
    st.info("Upload **Uren INPUT.xlsx** of vul een lokaal pad in via Ontwikkelmodus of laad een periode.")
    st.stop()

with st.spinner("Inlezen en berekenen..."):
    # Kies bron voor Excel
    excel_source = (st.session_state.dev_xlsx_path if (st.session_state.use_dev and st.session_state.dev_xlsx_path) else upl_xlsx)
    df_input = read_input_xlsx(excel_source)
    if df_input is None:
        df_input = pd.DataFrame()

    if not df_input.empty:
        df_input = df_input.drop_duplicates()
        df_input = df_input[~df_input['Naam SD WORX'].isin(exclude)]

    wp_map = (
        df_input[['Naam SD WORX','Werkplaats']].drop_duplicates()
        if not df_input.empty else pd.DataFrame(columns=['Naam SD WORX','Werkplaats'])
    )
    if not wp_map.empty:
        st.session_state['wp_by_name'] = dict(zip(wp_map['Naam SD WORX'], wp_map['Werkplaats']))

    # Kies bron voor XML
    xml_source = (st.session_state.dev_xml_path if (st.session_state.use_dev and st.session_state.dev_xml_path) else upl_xml)
    df_verlof = extract_verlof(xml_source) if xml_source else pd.DataFrame()
    if not df_verlof.empty:
        df_verlof = df_verlof[~df_verlof['Naam SD WORX'].isin(exclude)]

    # Dag-uren berekenen
    df_uren = calc_uren_per_dag(df_input, st.session_state.medewerkers_76, st.session_state.norm_per_dag)

# Zorg dat we DataFrames hebben
if not isinstance(df_uren, pd.DataFrame):
    df_uren = pd.DataFrame()
if not isinstance(wp_map, pd.DataFrame):
    wp_map = pd.DataFrame(columns=['Naam SD WORX','Werkplaats'])

# Werkplaats erbij
if not df_uren.empty and not wp_map.empty:
    df_uren = df_uren.merge(wp_map, on='Naam SD WORX', how='left')

# Verlof toevoegen als aparte rijen
if not df_uren.empty and not df_verlof.empty:
    verlof_rows = pd.DataFrame({
        'Naam SD WORX': df_verlof['Naam SD WORX'],
        'Datum': df_verlof['Datum'],
        'Weeknummer': df_verlof['Weeknummer'],
        'Eerste_start': pd.NaT,
        'Laatste_eind': pd.NaT,
        'Pauze_minuten': 0,
        'Type_dienst': 'Verlof',
        'Verlof_uren': df_verlof['Verlof_uren'],
        'Soll_pauze': 0,
        'Te_corrigeren_pauze': 0,
        'Reguliere_uren': 0,
        'Uren_128': 0,
        'Uren_147': 0,
        'Toeslag_uren': 0,
        'Zaterdag_uren': 0,
        'Zondag_uren': 0,
        'Ziekte_uren': 0
    })
    verlof_rows = verlof_rows.merge(wp_map, on='Naam SD WORX', how='left') if not wp_map.empty else verlof_rows
    temp = pd.concat([df_uren, verlof_rows], ignore_index=True)
    df_all = temp[temp['Werkplaats'].notna()].reset_index(drop=True) if 'Werkplaats' in temp.columns else temp.copy()
else:
    df_all = df_uren.copy()

# Normaliseer kolommen, bouw overzichten (volledige set)
if not df_all.empty:
    for col in ['Verlof_uren','Ziekte_uren']:
        if col not in df_all.columns: df_all[col] = 0.0
    keep = ['Naam SD WORX','Datum','Eerste_start','Laatste_eind','Weeknummer','Dienstduur_minuten','Pauze_minuten','Type_dienst','Soll_pauze',
            'Te_corrigeren_pauze','Norm_werkdag_minuten','Te_verlonen_minuten','Raw_minutes','Reguliere_uren','Uren_128','Uren_147','Toeslag_uren',
            'Zaterdag_uren','Zondag_uren','Ziekte_uren','Werkplaats','Verlof_uren']
    df_all = df_all[[c for c in df_all.columns if c in keep]].drop_duplicates()
    df_all = apply_corrections(df_all, st.session_state.corrections)
    df_input_sd, df_week, df_month = build_overviews(df_all, st.session_state.norm_per_dag)
else:
    df_input_sd = df_week = df_month = pd.DataFrame()

# ===========================
# Filters & gefilterde VIEW-data
# ===========================
c1, c2, c3, c4, c5 = st.columns([2,2,2,2,2])
with c1:
    medewerkers = sorted(df_all['Naam SD WORX'].dropna().unique().tolist()) if not df_all.empty else []
    sel_medewerker = st.selectbox("Medewerker", ["(Alle)"] + medewerkers)
with c2:
    werkplaatsen = sorted(df_all['Werkplaats'].dropna().unique().tolist()) if (not df_all.empty and 'Werkplaats' in df_all.columns) else []
    sel_wp = st.selectbox("Werkplaats", ["(Alle)"] + werkplaatsen)
with c3:
    min_d = df_all['Datum'].min().date() if not df_all.empty else datetime.today().date()
    max_d = df_all['Datum'].max().date() if not df_all.empty else datetime.today().date()
    dater = st.date_input("Datum bereik", value=(min_d, max_d))
with c4:
    st.write("")
    st.write("")
    reset = st.button("Reset filters")
with c5:
    months = sorted(df_all['Datum'].dt.to_period('M').astype(str).unique().tolist()) if not df_all.empty else []
    sel_month = st.selectbox("Maand", ["(Alle)"] + months)

if reset:
    sel_medewerker = sel_wp = "(Alle)"; dater = (min_d, max_d); sel_month = "(Alle)"

mask = pd.Series(True, index=df_all.index) if not df_all.empty else pd.Series(dtype=bool)
if not df_all.empty:
    if sel_medewerker != "(Alle)": mask &= df_all['Naam SD WORX'].eq(sel_medewerker)
    if sel_wp != "(Alle)" and 'Werkplaats' in df_all.columns: mask &= df_all['Werkplaats'].eq(sel_wp)
    if isinstance(dater, tuple) and len(dater)==2:
        d0, d1 = pd.to_datetime(dater[0]), pd.to_datetime(dater[1])
        mask &= (df_all['Datum']>=d0) & (df_all['Datum']<=d1)
    if sel_month != "(Alle)":
        mask &= df_all['Datum'].dt.to_period('M').astype(str).eq(sel_month)

# === VIEW-data op basis van alle filters (incl. Maand) ===
if not df_all.empty:
    df_all_view = df_all.loc[mask].copy()
    df_input_sd_view, df_week_view, df_month_view = build_overviews(df_all_view, st.session_state.norm_per_dag)
else:
    df_all_view = df_all.copy()
    df_input_sd_view = df_week_view = df_month_view = pd.DataFrame()

TAB_CORR, TAB_DAG, TAB_WEEK, TAB_MAAND, TAB_SD, TAB_DIENST, TAB_MISS = st.tabs([
    "🔧 Corrigeren", "🗓️ Uren per dag", "📆 WeekTotaal", "📅 MaandTotaal", "💻 SD Worx Export", "👔 Dienstverband",
    "⛳ Missende uren (<7u, ma–vr)"
])

with TAB_CORR:
    st.subheader("Per medewerker per dag corrigeren / toevoegen")
    if not medewerkers:
        st.info("Nog geen data ingeladen.")
    else:
        A,B,C,D,E = st.columns(5)
        with A: corr_med  = st.selectbox("Medewerker", options=medewerkers, key="corr_med")
        with B: corr_date = st.date_input("Datum", value=min_d, key="corr_date")
        with C: corr_type = st.selectbox("Type dienst", ["Reguliere dienst","Avond dienst","Zaterdag dienst","Zondag dienst","Verlof","Ziekte"], key="corr_type")
        with D: pauze_m   = st.number_input("Pauze (min)", min_value=0.0, step=5.0, value=0.0, key="corr_pauze")
        with E: verlof_u  = st.number_input("Verlof (uren)", min_value=0.0, step=0.5, value=0.0, key="corr_verlof")
        F,G,H = st.columns(3)
        with F: ziekte_u  = st.number_input("Ziekte (uren)", min_value=0.0, step=0.5, value=0.0, key="corr_ziekte")
        with G: reg_u     = st.number_input("Override Reguliere uren", min_value=0.0, step=0.25, value=0.0, key="corr_reg")
        with H: u128      = st.number_input("Override Uren 128%", min_value=0.0, step=0.25, value=0.0, key="corr_128")
        I,J = st.columns(2)
        with I: u147      = st.number_input("Override Uren 147%", min_value=0.0, step=0.25, value=0.0, key="corr_147")
        with J: save_corr = st.button("Opslaan/Toevoegen correctie")
        if save_corr:
            key = (corr_med, corr_date)
            cur = st.session_state.corrections.get(key, {})
            if pauze_m: cur['Pauze_minuten'] = float(pauze_m)
            if corr_type: cur['Type_dienst'] = corr_type
            if verlof_u: cur['Verlof_uren'] = float(verlof_u)
            if ziekte_u: cur['Ziekte_uren'] = float(ziekte_u)
            if reg_u:    cur['Reguliere_uren'] = float(reg_u)
            if u128:     cur['Uren_128'] = float(u128)
            if u147:     cur['Uren_147'] = float(u147)
            st.session_state.corrections[key] = cur
            st.success("Correctie opgeslagen. Pas een filter aan of gebruik Rerun om te verversen.")
        st.markdown("---")
        st.caption("Actieve correcties")
        if st.session_state.corrections:
            rows = [{"Naam SD WORX": k[0], "Datum": k[1], **v} for k,v in st.session_state.corrections.items()]
            st.dataframe(pd.DataFrame(rows).sort_values(["Naam SD WORX","Datum"]))
            if st.button("Alle correcties wissen"):
                st.session_state.corrections = {}; st.warning("Correcties gewist.")
        else:
            st.info("Nog geen correcties toegevoegd.")

with TAB_DAG:
    st.subheader("Uren per dag (na correcties)")
    if df_all_view.empty: st.info("Geen rijen in huidige selectie.")
    else:
        st.dataframe(df_all_view.sort_values(['Naam SD WORX','Datum']).assign(Datum=lambda x: x['Datum'].dt.date),
                     use_container_width=True, height=520)

with TAB_WEEK:
    st.subheader("WeekTotaal")
    if df_week_view.empty: st.info("Geen rijen in huidige selectie.")
    else:
        st.dataframe(df_week_view.sort_values(['Werkplaats','Naam SD WORX','Weeknummer']),
                     use_container_width=True, height=520)

with TAB_MAAND:
    st.subheader("MaandTotaal")
    if df_month_view.empty: st.info("Geen rijen in huidige selectie.")
    else:
        st.dataframe(df_month_view.sort_values(['Werkplaats','Naam SD WORX','Maand']),
                     use_container_width=True, height=520)

with TAB_SD:
    st.subheader("SD Worx – voorgestelde regels")
    if df_input_sd_view.empty: st.info("Geen rijen in huidige selectie.")
    else:
        st.dataframe(df_input_sd_view.sort_values(['Werkplaats','Naam SD WORX','Code']),
                     use_container_width=True, height=520)
        # Exporteer wat je ziet (gefilterde maand/filters)
        xls = write_excel_bytes(
            df_uren=df_all_view,
            df_verlof=df_verlof,   # verlof al verdisconteerd in df_all_view
            df_all=df_all_view,
            df_week=df_week_view,
            df_month=df_month_view,
            df_input_sd=df_input_sd_view
        )
        fname = f"uren_OUTPUT_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.xlsx"
        st.download_button("💾 Download Excel", data=xls, file_name=fname,
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # Optioneel: hele periode opslaan blijft de volledige set (df_all etc.) bewaren
        if st.button("💾 Opslaan naar periode (map)"):
            save_period(
                st.session_state["period_base"], st.session_state["periode"],
                upl_xlsx, st.session_state.dev_xlsx_path,
                upl_xml,  st.session_state.dev_xml_path,
                df_input if 'df_input' in locals() else pd.DataFrame(),
                df_verlof if 'df_verlof' in locals() else pd.DataFrame(),
                df_all, df_week, df_month, df_input_sd,
                st.session_state.norm_per_dag, st.session_state.corrections
            )
            st.success(f"Opgeslagen in: {os.path.join(st.session_state['period_base'], st.session_state['periode'])}")

with TAB_DIENST:
    st.subheader("Dienstverband – normuren per dag")
    if df_all_view.empty and df_all.empty:
        st.info("Nog geen data ingeladen.")
    else:
        base_df = df_all_view if not df_all_view.empty else df_all
        known = sorted(base_df['Naam SD WORX'].dropna().unique().tolist())
        if not st.session_state.norm_per_dag:
            st.session_state.norm_per_dag = {naam: (7.6 if naam in st.session_state.medewerkers_76 else 8.0) for naam in known}
        else:
            for naam in known:
                if naam not in st.session_state.norm_per_dag:
                    st.session_state.norm_per_dag[naam] = 7.6 if naam in st.session_state.medewerkers_76 else 8.0

        wp_by_name = st.session_state.get('wp_by_name', {})
        df_dienst = pd.DataFrame({
            'Naam SD WORX': known,
            'Werkplaats': [wp_by_name.get(n, '') for n in known],
            'Norm_uren_per_dag': [float(st.session_state.norm_per_dag.get(n, 8.0)) for n in known]
        })

        edited = st.data_editor(
            df_dienst,
            hide_index=True,
            use_container_width=True,
            column_config={
                'Norm_uren_per_dag': st.column_config.NumberColumn("Norm uren/dag", min_value=0.0, max_value=12.0, step=0.1, help="Uren per werkdag"),
            }
        )
        col1, col2 = st.columns([1,3])
        with col1:
            if st.button("Opslaan dienstverbanden"):
                st.session_state.norm_per_dag = {row['Naam SD WORX']: float(row['Norm_uren_per_dag']) for _, row in edited.iterrows()}
                st.success("Dienstverbanden opgeslagen – berekeningen worden herberekend.")
                st.rerun()
        with col2:
            new_default = st.number_input("Standaard norm (uren/dag) voor nieuwe namen", min_value=0.0, max_value=12.0, step=0.1, value=8.0)
            if st.button("Zet iedereen op standaard"):
                st.session_state.norm_per_dag = {naam: float(new_default) for naam in known}
                st.warning("Alle normuren op standaard gezet.")
                st.rerun()

with TAB_MISS:
    st.subheader("Missende uren – minder dan 7 uur verantwoord (ma–vr)")
    # Werk op basis van view (filters + maand)
    if df_all_view is None or df_all_view.empty:
        st.info("Geen rijen in huidige selectie.")
    else:
        _df = df_all_view.copy()
        # Alleen werkdagen (ma=0 .. vr=4)
        _df = _df[_df['Datum'].dt.weekday <= 4]

        if _df.empty:
            st.info("Geen werkdagen in de huidige selectie.")
        else:
            # Zorg dat benodigde kolommen bestaan
            for c in ['Reguliere_uren','Uren_128','Uren_147','Verlof_uren','Ziekte_uren']:
                if c not in _df.columns: _df[c] = 0.0

            # Som per medewerker per dag
            grp = _df.groupby(['Werkplaats','Naam SD WORX','Datum','Weeknummer'], as_index=False)[
                ['Reguliere_uren','Uren_128','Uren_147','Verlof_uren','Ziekte_uren']
            ].sum()

            # Totaal verantwoord (zonder Toeslag-uren) -> inclusief Verlof & Ziekte
            grp['Verantwoord_uren'] = (
                grp['Reguliere_uren'] + grp['Uren_128'] + grp['Uren_147'] +
                grp['Verlof_uren'] + grp['Ziekte_uren']
            ).round(2)

            # Persoonlijke norm-uren per dag ophalen
            def _default_norm(name: str) -> float:
                # default 7.6u als in 7.6-lijst, anders 8.0u
                return 7.6 if name in st.session_state.get('medewerkers_76', set()) else 8.0

            norm_map = {
                naam: float(st.session_state.get('norm_per_dag', {}).get(naam, _default_norm(naam)))
                for naam in grp['Naam SD WORX'].unique()
            }
            grp['Norm_uren_per_dag'] = grp['Naam SD WORX'].map(norm_map).astype(float).round(2)

            # Missende uren per dag = Norm - Verantwoord (niet negatief)
            grp['Missende_uren'] = (grp['Norm_uren_per_dag'] - grp['Verantwoord_uren']).clip(lower=0).round(2)

            # Alleen rijen met <7u verantwoord (zoals je oorspronkelijke filter), maar mét extra kolommen
            miss = grp[grp['Verantwoord_uren'] < 7].copy()

            if miss.empty:
                st.success("Geen missende uren: iedereen ≥ 7 uur op de geselecteerde werkdagen 🎉")
            else:
                # Kolomvolgorde netter maken
                cols = [
                    'Werkplaats','Naam SD WORX','Datum','Weeknummer',
                    'Norm_uren_per_dag','Verantwoord_uren','Verlof_uren','Missende_uren',
                    'Reguliere_uren','Uren_128','Uren_147','Ziekte_uren'
                ]
                cols = [c for c in cols if c in miss.columns]  # veilig
                miss = miss[cols].sort_values(['Werkplaats','Naam SD WORX','Datum'])
                st.dataframe(
                    miss.assign(Datum=lambda x: x['Datum'].dt.date),
                    use_container_width=True, height=520
                )
                csv_bytes = miss.to_csv(index=False).encode('utf-8')
                st.download_button(
                    "⬇️ Download missende-uren (CSV)",
                    data=csv_bytes,
                    file_name=f"missende_uren_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.csv",
                    mime="text/csv"
                )

# Sidebar save (na berekening) — dit bewaart de volledige periode (onafhankelijk van filters)
if save_period_btn_sidebar and not (df_all is None or df_all.empty):
    save_period(
        st.session_state["period_base"], st.session_state["periode"],
        upl_xlsx, st.session_state.dev_xlsx_path,
        upl_xml,  st.session_state.dev_xml_path,
        df_input if 'df_input' in locals() else pd.DataFrame(),
        df_verlof if 'df_verlof' in locals() else pd.DataFrame(),
        df_all, df_week, df_month, df_input_sd,
        st.session_state.norm_per_dag, st.session_state.corrections
    )
    st.success(f"Opgeslagen in: {os.path.join(st.session_state['period_base'], st.session_state['periode'])}")

st.caption("Start dit bestand met de Play-knop of `python <bestand>.py`: het opent 1× de Streamlit-app. Ontwikkelmodus laat lokale paden toe zonder upload.")
